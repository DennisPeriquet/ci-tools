package helpdesk_faq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	faqConfigMap = "helpdesk-faq"
	ci           = "ci"
)

type FaqItemClient interface {
	GetSerializedFAQItems() ([]string, error)
	GetFAQItemIfExists(timestamp string) (*FaqItem, error)
	UpsertItem(item FaqItem) error
	RemoveItem(timestamp string) error
}

func NewCMClient(kubeClient ctrlruntimeclient.Client) ConfigMapClient {
	return ConfigMapClient{kubeClient: kubeClient}
}

type ConfigMapClient struct {
	kubeClient  ctrlruntimeclient.Client
	cachedItems []string
	lastReload  time.Time
}

func (c *ConfigMapClient) GetSerializedFAQItems() ([]string, error) {
	fifteenMinutesFromLastCacheReload := c.lastReload.Add(time.Minute * 15)
	if len(c.cachedItems) > 0 && time.Now().Before(fifteenMinutesFromLastCacheReload) {
		logrus.Debug("returning faq items from cache")
		return c.cachedItems, nil
	}
	logrus.Debug("reloading faq items from configmap")
	configMap, err := c.getConfigMap()
	if err != nil {
		return nil, err
	}
	if configMap.Data == nil {
		return nil, nil
	}
	var items []string
	for _, item := range configMap.Data {
		items = append(items, item)
	}
	c.cachedItems = items
	c.lastReload = time.Now()
	return items, nil
}

func (c *ConfigMapClient) GetFAQItemIfExists(timestamp string) (*FaqItem, error) {
	configMap, err := c.getConfigMap()
	if err != nil {
		return nil, fmt.Errorf("unable to get configmap: %w", err)
	}
	rawFaqItem := configMap.Data[timestamp]
	if rawFaqItem == "" {
		return nil, nil
	}
	faqItem := &FaqItem{}
	if err = json.Unmarshal([]byte(rawFaqItem), faqItem); err != nil {
		return nil, fmt.Errorf("unable to unmarshall faqItem: %w", err)
	}
	return faqItem, nil
}

func (c *ConfigMapClient) UpsertItem(item FaqItem) error {
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("unable to marshal faqItem to json: %w", err)
	}
	configMap, err := c.getConfigMap()
	if err != nil {
		return fmt.Errorf("unable to get configmap: %w", err)
	}
	configMap.Data[item.Timestamp] = string(data)
	err = c.kubeClient.Update(context.TODO(), configMap)
	if err != nil {
		return fmt.Errorf("unable to update helpdesk-faq config map: %w", err)
	}

	return nil
}

func (c *ConfigMapClient) RemoveItem(timestamp string) error {
	configMap, err := c.getConfigMap()
	if err != nil {
		return fmt.Errorf("unable to get configmap: %w", err)
	}
	delete(configMap.Data, timestamp)
	err = c.kubeClient.Update(context.TODO(), configMap)
	if err != nil {
		return fmt.Errorf("unable to update helpdesk-faq config map: %w", err)
	}

	return nil
}

func (c *ConfigMapClient) getConfigMap() (*v1.ConfigMap, error) {
	configMap := &v1.ConfigMap{}
	if err := c.kubeClient.Get(context.TODO(), types.NamespacedName{Namespace: ci, Name: faqConfigMap}, configMap); err != nil {
		return nil, fmt.Errorf("failed to get configMap %s: %w", faqConfigMap, err)
	}
	return configMap, nil
}
