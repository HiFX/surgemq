package persistence

//persistence implements the functionalities for saving the chat

//author :  shalin LK <shalinlk@hifx.co.in>
import (
	"github.com/go-redis/redis"
	"strings"
	"sort"
	"crypto/md5"
	"hash"
	"github.com/HiFX/surgemq/models"
	"errors"
	"fmt"
	"time"
	"encoding/json"
	"strconv"
)

const (
	PARTICIPANTS_SEPERATOR = "|"
	KALAPILA_MAX_QOS       = 2
	USER_GROUP_PREFIX      = "ug"
	BUDDY_LIST_PREFIX      = "bl"
	NICK_SHADOW_PREFIX     = "ns"
	CHAT_TOKEN_PREFIX      = "ct"
	USER_HISTORY_PREFIX    = "uh"
	CHAT_HISTORY_PREFIX    = "ch"
	USER_PROFILE_PREFIX    = "up"
)

//todo : may have to implement a close function for winding up the
//todo : operations, closing the connections and closing the clannels
//todo : for perfect synchronization;
type Redis struct {
	client *redis.Client
	hasher      hash.Hash
	flushTunnel chan *flushPack
}

func NewRedis(host, pass string, db int) (*Redis, error) {
	//prepare client
	rClient := redis.NewClient(&redis.Options{
	Addr : host,
	Password : pass,
	DB : int64(db),
})
	_, err := rClient.Ping().Result()
	if err != nil {
		return nil, err
	}
	newRedis := &Redis{client : rClient}
	//prepare hasher
	newRedis.hasher = md5.New()
	//prepare flushing channel
	//todo : take care of the channel buffer; size should be non blocking
	newRedis.flushTunnel = make(chan *flushPack)
	newRedis.flusher()
	return newRedis, nil
}

//NewSubscription generates a new topic (here after known as
//shadow) for the given client topic, regeisters it in the
//persistence, registers the users of the new topic and
//returns the shadow;
//The param clientTopic is assumed to follow the format of
//user_id_1|user_id_2|user_id_3
func (this *Redis) Subscribe(clientTopic, userId string, qos int, authorizer func(...string) bool) (error) {
	buddies, nickName, err := this.ClientGroupBuddies(clientTopic)
	if err != nil {
		return err
	}
	//authorization
	if !authorizer(buddies...) {
		return errors.New("unauthorized chat group")
	}
	_, err = this.groupShadow(buddies, nickName, clientTopic, userId, qos)
	return err
}

//Unsubscribe handles the unsubsribing of the client from a user group;
func (this *Redis) Unsubscribe(clientTopic, unsubscriber string) (error) {
	buddies, nickName, err := this.ClientGroupBuddies(clientTopic)
	if err != nil {
		return err
	}
	return this.unsubscribe(buddies, nickName, clientTopic, unsubscriber)
}

//Set for setting values in db; returns error
func (this *Redis) SetChatToken(userId string, token models.Token) error {
	return this.setChatToken(userId, token)
}

//Get for getting values from db; returns value and error
func (this *Redis) GetChatToken(userId string) (string, error) {
	return this.getChatToken(userId)
}

//Flush : for storing a message in the persistence module;
//Flush expects topic used in client side and the message.
//Flush will recover the shadow for the group and store the
//message;
func (this *Redis) Flush(groupName string, msg *models.Message) (error) {
	_, nickName, err := this.ClientGroupBuddies(groupName)
	if err != nil {
		return err
	}
	shadow, err := this.getNickShadow(nickName)
	if err != nil {
		return err
	}
	this.dumpInFlushSink(shadow, msg.Serialize())
	return nil
}

//Scan implements scanning of the chat history
func (this *Redis) Scan(userId, clientGroupLabel string, offset, count int) ([]models.Message, error) {
	_, nickName, err := this.ClientGroupBuddies(clientGroupLabel)
	if err != nil {
		//todo : deal error
	}
	return this.scan(userId, nickName, offset, count)
}

//ChatList returns identifiers for all the unique chats of the user;
//An identifier is usually a string which is actually a combination of
//ids of the participating clients seperated by |. A client should exchange
//a chat identifier for loading the chat history;
func (this *Redis) ChatList(userId string) ([]string, error) {
	//todo : parsing of error may be needed for checking whether
	//todo : the error is of not found kind;
	return this.client.HKeys(userId).Result()
}

func (this *Redis) ClientSubscriptions(userId string) (map[string]int, error) {
	return this.getUserGroupNames(userId)
}

func (this *Redis) unsubscribe(buddies []string, nickName, clientGroupName, userId string) error {
	//	fmt.Println("An unsubscribe Message from : ", userId, " for : ", nickName)

	activeBuddies, err := this.getBuddyList(nickName)
	if err != nil {
		return err
	}
	if !foundInArray(activeBuddies, userId) {
		//invoking user is no longer active subscriber of the group; nothing to do
		fmt.Println("invoking user is no longer active subscriber of the group")
		return nil
	}
	err = this.removeFromBuddyList(nickName, userId)
	if err != nil {
		fmt.Println("unsubscribe failed; failed to remve from buddy list")
		return errors.New("unsubscribe failed; failed to remve from buddy list")
	}
	newNick := fmt.Sprintf("%s_%d", nickName, time.Now().UTC().Unix())

	//rename all existing nickName for all participants
	for _, user := range activeBuddies {
		err := this.renameNickOfUserChat(user, nickName, newNick)
		if err != nil {
			return err
		}
		err = this.renameNickOfUserGroup(user, nickName, newNick)
		if err != nil {
			return err
		}
	}

	//remove this entry from user group of this user
	err = this.removeFromUserGroup(userId, newNick)
	if err != nil {
		return err
	}
	//rename nick name of buddy list
	err = this.renameBuddyList(nickName, newNick)
	if err != nil {
		return err
	}
	//create new topic for the remaining
	remainingBuddies, err := this.getBuddyList(newNick)
	if err != nil {
		return err
	}
	fmt.Println("Unsubscription Success ; Remaining Users :", remainingBuddies)
	if len(remainingBuddies) <= 1  {
		//the solo talker; nothing to do; return
		return nil
	}
	_ ,err = this.newShadow(remainingBuddies, nickName, clientGroupName, 1)
	return err
}

func (this *Redis) groupShadow(buddies []string, nickName, clientGroup, userId string, qos int) (string, error) {
	fmt.Println("groupShadow invoked for : User ", userId, "; Nick Name : ", nickName, "; Buddies : ", buddies)
	shadow, err := this.getNickShadow(nickName)
	fmt.Println("Existing Shadow of nick name : ", shadow)
	if err == nil && len(shadow) > 0 {
		fmt.Println("Shadow is exiting for clientGroup : ", clientGroup)
		//todo : refine error to further level for detecting s/m failure
		//a shadow already exist for this nick name;
		//get active users of this group and add calling
		//member only to the list under new shadow;
		activeUsers, err := this.getBuddyList(nickName)
		if err != nil {
			//todo : deal error
		}
		if len(activeUsers) == len(buddies) {
			//false subscribe request
			fmt.Println("This is a flase subscription")
			return shadow, nil
		}
		if foundInArray(activeUsers, userId) {
			//this is a flase request; user is already present in subscription
			return shadow, nil
		}
		if foundInArray(buddies, userId) {
			//requesting user is no longer part of the existing topic under the nick name;
			//rename existing entries
			//	1. nickName - shadow mapping : will automatically replaced on new shadow creation
			//	2. user - nickName - shadow mapping
			//	3. user - nickName - clientGroup mapping
			now := time.Now().UTC().Unix()
			newNick := fmt.Sprintf("%s_%d", nickName, now)
			for _, user := range activeUsers {
				//2...
				err := this.renameNickOfUserChat(user, nickName, newNick)
				if err != nil {
					//todo : deal error
				}
				//3...
				err = this.renameNickOfUserGroup(user, nickName, newNick)
				if err != nil {
					//todo : deal error ; cannot proceed
				}
			}
			//	4. buddy list, (nickName - active_users) mapping
			err = this.renameBuddyList(nickName, newNick)
			if err != nil {
				//todo : deal error; cannot be proceeded
			}
			//add the calling user to the list, leaving all unsubscribed users as it is;
			buddies = append(activeUsers, userId)
		}
	}
	//create and register a new shadow
	fmt.Println("Invoking New Shadow Maker")
	return this.newShadow(buddies, nickName, clientGroup, qos)
}

func (this *Redis) scan(userId, nickName string, offSet, count int) ([]models.Message, error) {
	shadow, err := this.client.HGet(userId, nickName).Result()
	if err != nil {
		//todo : deal error
		return []models.Message{}, err
	}
	list, err := this.client.LRange(shadow, int64(offSet), int64(count)).Result()
	if err != nil {
		//todo : deal error
		return []models.Message{}, err
		//return
	}
	result := make([]models.Message, len(list))
	for i, msg := range list {
		msgHolder := models.Message{}
		json.Unmarshal([]byte(msg), &msgHolder)
		result[i] = msgHolder
	}
	return result, nil
}

//newShadow generates a new shadow topic (for internal use) for the
//given users and registers the same in topic list; It also registers
//an entry in persistence with the key being the shadow and value
//being the users listening on this topic
func (this *Redis) newShadow(buddies []string, nickName, clientGroupName string, qos int) (string, error) {
	//todo : perform authorization of the group members here
	this.hasher.Reset()
	//	_, err := this.hasher.Write([]byte(flatGroup))
	//	if err != nil {
	//		return "", err
	//	}
	//	shadow := string(this.hasher.Sum(nil))
	//todo : code is experimental
	shadow := strconv.Itoa(int(time.Now().UTC().Unix()))
	//1. register the shadow in topic list mapper
	err := this.setNickShadow(nickName, shadow)
	if err != nil {
		return shadow, err
	}
	//2. register active users of the shadow as list
	err = this.setBuddyList(nickName, buddies)
	if err != nil {
		return shadow, err
	}
	//3. register shadow for all participants under the nick name.
	//4. register user group name under nick name for topic -
	//re-loading on warm up after a down time for all participants
	for _, userId := range buddies {
		//3. ...
		err := this.setUserChatHistory(userId, nickName, shadow)
		if err != nil {
			return shadow, err
		}
		//4. ...
		if err = this.setUserGroup(userId, nickName, clientGroupName, qos); err != nil {
			return shadow, err
		}
	}

	return shadow, nil
}
func (this *Redis) removeFromUserGroup(userId, nickName string) error {
	fmt.Println("Removing from user group for : ", userId, "; Nick Name : ", nickName)
	label := fmt.Sprintf("%s_%s", USER_GROUP_PREFIX, userId)
	_, err := this.client.HDel(label, nickName).Result()
	if err == nil || err == redis.Nil{
		return nil
	}
	return err
}

func (this *Redis) setUserGroup(userId, nickName, userGroupName string, qos int) error {
	if qos > KALAPILA_MAX_QOS {
		return errors.New("invalid qos")
	}
	//attach prefix to the userId for distinguishing user group
	label := fmt.Sprintf("%s_%s", USER_GROUP_PREFIX, userId)
	//attach qos with the client group name
	value := fmt.Sprintf("%d%s%s", qos, PARTICIPANTS_SEPERATOR, userGroupName)
	fmt.Println("Set user Group ; label : ", label, " ; value : ", value, " ; Nick name : ", nickName)
	_, err := this.client.HSet(label, nickName, value).Result()
	return err
}

func (this *Redis) getUserGroupNames(userId string) (map[string]int , error) {
	result := make(map[string]int)
	label := fmt.Sprintf("%s_%s", USER_GROUP_PREFIX, userId)
	values, err := this.client.HVals(label).Result()
	if err != nil {
		//todo : deal error
		return result, err
	}

	fmt.Println("Get user group names for : ", userId)
	fmt.Println("length of value : ", len(values))
	for _, value := range values {
		fmt.Println(" value : ", value)
		value = strings.TrimSpace(value)
		if len(value) > 0 {
			splitAt := strings.Index(value, PARTICIPANTS_SEPERATOR)
			qosStr := value[0:splitAt]
			groupName := value[splitAt+1:]
			qos, _ := strconv.Atoi(qosStr)
			result[groupName] = qos
		}
	}
	return result, nil
}

func (this *Redis) renameNickOfUserGroup(userId, oldNick, newNick string) error {
	fmt.Println("Renaming Nick of user group for : ", userId, "; OldNick : ", oldNick, "; NewNick : ", newNick)
	label := fmt.Sprintf("%s_%s", USER_GROUP_PREFIX, userId)
	clientGroupName, err := this.client.HGet(label, oldNick).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	_, err = this.client.HDel(label, oldNick).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if len(clientGroupName) == 0 {
		fmt.Println("Renaming Client Group Name is empty; returning..")
		return nil
	}
	_, err = this.client.HSet(label, newNick, clientGroupName).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (this *Redis) dumpInFlushSink(shadow string, message []byte) {
	//todo : tunneling should be restricted to avoid writing on a closed channel
	this.flushTunnel <- &flushPack{shadow : shadow, load : message}
	return
}

//flush listens on the channel for input messages and flushes it to db
func (this *Redis) flusher() {
	//todo : this method does not mind the order of incoming messages
	go func() {
		for {
			pack := <-this.flushTunnel
			fmt.Println("Flushing : ", pack.key(), " : ", pack.value())
			err := this.client.LPush(pack.key(), pack.value())
			if err != nil {
				//todo : deal error
				//todo : 1. block subsequent writes
				//todo : 2. report error
				//todo : 3. try recovery
			}
		}
	}()
	return
}

func (this *Redis) setBuddyList(nickName string, buddies []string) (error) {
	label := fmt.Sprintf("%s_%s", BUDDY_LIST_PREFIX, nickName)
	fmt.Println("setting buddy list : label : ", label, "; buddies : ", buddies)
	_, err := this.client.SAdd(label, buddies...).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (this *Redis) getBuddyList(nickName string) ([]string, error) {
	label := fmt.Sprintf("%s_%s", BUDDY_LIST_PREFIX, nickName)
	res, err := this.client.SMembers(label).Result()
	fmt.Println("Get from Buddy List for label : ", label)
	if err == nil || err == redis.Nil {
		return res, nil
	}
	return res, err
}

func (this *Redis) renameBuddyList(oldNick, newNick string) (error) {
	oldNick = fmt.Sprintf("%s_%s", BUDDY_LIST_PREFIX, oldNick)
	newNick = fmt.Sprintf("%s_%s", BUDDY_LIST_PREFIX, newNick)
	_, err := this.client.Rename(oldNick, newNick).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (this *Redis) removeFromBuddyList(nickName, user string) (error) {
	fmt.Println("Removing buddy from list; Nick Name :", nickName, "; User : ", user)
	label := fmt.Sprintf("%s_%s", BUDDY_LIST_PREFIX, nickName)
	_, err := this.client.SRem(label, user).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}
func (this *Redis) setNickShadow(nickName, shadow string) (error) {
	label := fmt.Sprintf("%s_%s", NICK_SHADOW_PREFIX, nickName)
	_, err := this.client.Set(label, shadow, time.Duration(0)).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (this *Redis) getNickShadow(nickName string) (string, error) {
	label := fmt.Sprintf("%s_%s", NICK_SHADOW_PREFIX, nickName)
	shadow, err := this.client.Get(label).Result()
	if err == nil || err == redis.Nil {
		return shadow, nil
	}
	return shadow, err
}

func (this *Redis) renameNickOfNickShadow(oldNick, newNick string) (error) {
	oldNick = fmt.Sprintf("%s_%s", NICK_SHADOW_PREFIX, oldNick)
	newNick = fmt.Sprintf("%s_%s", NICK_SHADOW_PREFIX, newNick)
	_, err := this.client.Rename(oldNick, newNick).Result()
	return err
}

func (this *Redis) setChatToken(userId string, token models.Token) (error) {
	//todo : save token for user information
	label := fmt.Sprintf("%s_%s", CHAT_TOKEN_PREFIX, userId)
	_, err := this.client.Set(label, token.Sub, time.Duration(0)).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (this *Redis) getChatToken(userId string) (string, error) {
	label := fmt.Sprintf("%s_%s", CHAT_TOKEN_PREFIX, userId)
	return this.client.Get(label).Result()
}

func (this *Redis) setUserChatHistory(userId, nickName, shadow string) (error) {
	label := fmt.Sprintf("%s_%s", USER_HISTORY_PREFIX, userId)
	_, err := this.client.HSet(label, nickName, shadow).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (this *Redis) userChatHistoryForNick(userId, nickName string) (string, error) {
	label := fmt.Sprintf("%s_%s", USER_HISTORY_PREFIX, userId)
	return this.client.HGet(label, nickName).Result()
}

func (this *Redis) renameNickOfUserChat(userId, oldNick, newNick string) (error) {
	label := fmt.Sprintf("%s_%s", USER_HISTORY_PREFIX, userId)
	userShadow, err := this.client.HGet(label, oldNick).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	_, err = this.client.HDel(label, oldNick).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	_, err = this.client.HSet(label, newNick, userShadow).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

//clientGroup buddies returns the names of participants in a client chat group
//along with the internally used name(group nick name) to represent the client group;
func (this *Redis) ClientGroupBuddies(clientTopic string) ([]string, string, error) {
	participants := strings.Split(clientTopic, PARTICIPANTS_SEPERATOR)
	if len(participants) < 2 {
		return []string{}, "", errors.New("invalid number of participants")
	}
	//todo : implement participants authenticity check
	buddies := parties(participants)
	sort.Sort(buddies)
	return buddies, strings.Join(buddies, "|"), nil
}

type flushPack struct {
	shadow  string
	load    []byte
}

func (this *flushPack) key() string {
	return this.shadow
}

func (this *flushPack) value() string {
	return string(this.load)
}

//parties implements sort.Sort
type parties []string

func (this parties) Len() int {
	return len(this)
}

func (this parties) Swap(i, j int) {
	temp := this[i]
	this[i] = this[j]
	this[j] = temp
}

func (this parties) Less(i, j int) bool {
	return this[i] < this[j]
}

func foundInArray(holder []string, child string) bool {
	for _, elem := range holder {
		if elem == child {
			return true
		}
	}
	return false
}
