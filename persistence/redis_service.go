package persistence

//persistence implements the functionalities for saving the chat

//author :  shalin LK <shalinlk@hifx.co.in>
import (
	"github.com/go-redis/redis"
	"strings"
	"sort"
	"crypto/md5"
	"hash"
)

const (
	PARTICIPANTS_SEPERATOR = "|"
)

type Redis struct {
	client *redis.Client
	hasher hash.Hash
}

func NewRedis(host, pass string, db int) (*Redis, error) {
	rClient := redis.NewClient(&redis.Options{
	Addr : host,
	Password : pass,
	DB : int64(db),
	})
	_, err := rClient.Ping().Result()
	if err != nil {
		return nil, err
	}
	newRedis := &Redis{rClient}
	newRedis.hasher = md5.New()
	return newRedis, nil
}

//NewSubscription generates a new topic (here after known as
//shadow) for the given client topic, regeisters it in the
//persistence, registers the users of the new topic and
//returns the shadow;
//The param clientTopic is assumed to follow the format of
//user_id_1|user_id_2|user_id_3
func (this *Redis) NewSubscription(clientTopic string) {
	return this.TopicShadow(clientTopic)
}

//ShadowTopic returns the intenally used topic name for the
//flat name given; If no topic exist for the given string,
//ShadowTopic will generate a new topic (here after known
//as shadow), register it in the persistence and will be returned;
func (this *Redis) TopicShadow(clientTopic string){
	participants := strings.Split(clientTopic, PARTICIPANTS_SEPERATOR)
	if len(participants) < 2 {
		//todo : invalid number of participants
		return
	}
	return this.GroupShadow(participants)
}

//GroupShadow returns the internally used topic name for
//the set of participants in a group; If an internal name
//exist for the group, it will be returned, otherwise,
//GroupShadow will generate a new topic, topic (here after known
//as shadow), register it in the persistence and will be returned;
func (this *Redis) GroupShadow(group []string) {
	buddies := sort.Sort(parties(group))
	//check whether a shadow exist for the group
	val, readError := this.client.Get(strings.Join(buddies, "")).Result()
	//todo : consider the error case here; another level of refining may be needed
	if readError == nil {
		return val, nil
	}
	//create and register a new shadow
	return this.newShadow(flatGroup)
}

//newShadow generates a new shadow topic (for internal use) for the
//given users and registers the same in topic list; It also registers
//an entry in persistence with the key being the shadow and value
//being the users listening on this topic
func (this *Redis) newShadow(groupLabel string, groupMembers []string) (string, error){
	this.hasher.Reset()
	_, err := this.hasher.Write([]byte(connectedBuddies))
	if err != nil {
		return "", err
	}
	shadow := string(this.hasher.Sum(nil))
	//register the shadow in topic list mapper
	setError := this.client.Set(flatGroup, shadow).Err()
	if setError != nil {
		return "", setError
	}
	//register the shadow users as list
	this.client.LPush(groupLabel, groupMembers...)
	return shadow, nil
}

//parties implements sort.Sort
type parties []string

func (this parties) Len() {
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
