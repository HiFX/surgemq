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
	newRedis.flush()
	return newRedis, nil
}

//NewSubscription generates a new topic (here after known as
//shadow) for the given client topic, regeisters it in the
//persistence, registers the users of the new topic and
//returns the shadow;
//The param clientTopic is assumed to follow the format of
//user_id_1|user_id_2|user_id_3
func (this *Redis) NewSubscription(clientTopic string) (error) {
	_, err := this.TopicShadow(clientTopic)
	return err
}

//ShadowTopic returns the intenally used topic name for the
//flat name given; If no topic exist for the given string,
//TopicShadow will generate a new topic (here after known
//as shadow), register it in the persistence and will be returned;
func (this *Redis) TopicShadow(clientTopic string) (string, error) {
	buddies, nickName, err := this.clientGroupBuddies(clientTopic)
	if err != nil {
		return "", err
	}
	return this.groupShadow(buddies, nickName)
}

//GroupShadow returns the internally used topic name for
//the set of participants in a group; If an internal name
//exist for the group, it will be returned, otherwise,
//GroupShadow will generate a new topic (here after known
//as shadow), register it in the persistence and will be returned;
//A shadow can be changed at any point in execution even by any
//other paralley running machine. So shadow can't be kept in
//memory without a two way synch up mechanism;
func (this *Redis) GroupShadow(group []string) (string, error){
	flatGroup := strings.Join(group, PARTICIPANTS_SEPERATOR)
	buddies, nickName, err := this.clientGroupBuddies(flatGroup)
	if err != nil {
		return "", err
	}
	return this.groupShadow(buddies, nickName)
}

//Unsubscribe handles the unsubsribing of the client from a user group;
func (this *Redis) Unsubscribe(unsubscriber, flatGroupName string) {
	//todo
}

//Flush : for storing a message in the persistence module;
//Flush expects topic used in client side and the message.
//Flush will recover the shadow for the group and store the
//message;
func (this *Redis) Flush(groupLabel string, msg *models.Message) (error) {
	shadow, err := this.TopicShadow(groupLabel)
	if err != nil {
		return err
	}
	this.dumpInFlushSink(shadow, msg.Serialize())
	return nil
}

//Scan implements scanning of the chat history
func (this *Redis) Scan(userId, clientGroupLabel string, offset, count int)([]models.Message, error) {
	_, nickName, err := this.clientGroupBuddies(clientGroupLabel)
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


func (this *Redis) groupShadow(buddies []string, flatGroup string) (string, error) {
	val, readError := this.client.Get(flatGroup).Result()
	//todo : consider the error case here; another level of refining may be needed
	if readError == nil {
		return val, nil
	}
	//create and register a new shadow
	return this.newShadow(flatGroup, buddies)
}

func (this *Redis) scan(userId, nickName string, offSet, count int)([]models.Message, error) {
	shadow, err := this.client.HGet(userId, nickName).Result()
	if err != nil {
		//todo : deal error
		fmt.Println("Error in HGet for ", userId, " under group ", nickName)
		return []models.Message{}, err
	}
	fmt.Println("Shadaow obtained in scan : ", shadow)
	list, err := this.client.LRange(shadow, int64(offSet), int64(count)).Result()
	if err != nil {
		//todo : deal error
		fmt.Println("Error in LRange for shadow : ", shadow)
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
func (this *Redis) newShadow(flatGroup string, groupMembers []string) (string, error) {
	//todo : perform authorization of the group members here
	this.hasher.Reset()
//	_, err := this.hasher.Write([]byte(flatGroup))
//	if err != nil {
//		return "", err
//	}
//	shadow := string(this.hasher.Sum(nil))
	//todo : code is experimental
	shadow := strconv.Itoa(int(time.Now().UTC().Unix()))
	//register the shadow in topic list mapper
	setError := this.client.Set(flatGroup, shadow, time.Duration(0)).Err()
	if setError != nil {
		return "", setError
	}
	//register active users of the shadow as list
	this.client.LPush(flatGroup, groupMembers...)
	//register shadow for all participants under the flatgroup name
	for _, userId := range groupMembers {
		status, err := this.client.HExists(userId, flatGroup).Result()
		if err != nil {
			//todo : deal error
			fmt.Println("HExists Error..")
			continue
		}
		if status {
			fmt.Println("HExist : Topic Exist for this user")
			//todo : replace current value with an append
			continue
		}else{
			fmt.Println("regestering shadow topic for : ", userId)
			_, err = this.client.HSet(userId, flatGroup, shadow).Result()
			if err != nil {
				//todo : deal error
				fmt.Println("Error in regestering shadow topic for : ", userId, "; Error : ", err)
			}
		}
	}
	return shadow, nil
}

func (this *Redis) dumpInFlushSink(shadow string, message []byte) {
	//todo : tunneling should be restricted to avoid writing on a closed channel
	this.flushTunnel <- &flushPack{shadow : shadow, load : message}
	return
}

//flush listens on the channel for input messages and flushes it to db
func (this *Redis) flush() {
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

//clientGroup buddies returns the names of participants in a client chat group
//along with the internally used name to represent the client group;
func (this *Redis) clientGroupBuddies(clientTopic string) ([]string, string, error){
	participants := strings.Split(clientTopic, PARTICIPANTS_SEPERATOR)
	if len(participants) < 2 {
		return []string{}, "", errors.New("invalid number of participants")
	}
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
