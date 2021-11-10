package main

import (
	"github.com/pkg/errors"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/sirupsen/logrus"
	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	pluginName          = "AwsBackupItemAction"
	zoneTopologyKey     = "topology.ebs.csi.aws.com/zone"
	zoneLabelDeprecated = "failure-domain.beta.kubernetes.io/zone"
	zoneLabel           = "topology.kubernetes.io/zone"
)

// BackupTimeAction here add "zone" label to a PV resource
// based on the node affinity value if the label is not already set.
type BackupTimeAction struct {
	log logrus.FieldLogger
}

func newBackupItemAction(logger logrus.FieldLogger) *BackupTimeAction {
	return &BackupTimeAction{log: logger}
}

func (bta *BackupTimeAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumes"},
	}, nil
}

func (bta *BackupTimeAction) Execute(item runtime.Unstructured, backup *v1.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	bta.log.Info("Executing AWS bta")
	var pv corev1api.PersistentVolume
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &pv); err != nil {
		return nil, nil, errors.Wrap(err, "unable to convert unstructured to pv")
	}

	log := bta.log.WithFields(logrus.Fields{"persistentVolume": pv.Name, "plugin": pluginName})
	zone, found := pv.Labels[zoneLabel]
	if !found {
		zone = pv.Labels[zoneLabelDeprecated]
	}

	if zone != "" {
		log.Infof("Found zone info from label: %s", zone)
		return item, nil, nil
	}
	zone = bta.getZoneFromNodeAffinity(pv.Spec)
	if zone == "" {
		log.Infof("Zone not found from node affinity requirements")
		return item, nil, nil
	}
	if pv.Labels == nil {
		pv.Labels = map[string]string{}
	}
	pv.Labels[zoneLabel] = zone
	log.Infof("Added the Availability Zone from node affinity requirements, key: %s, value: %s", zoneLabel, zone)
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pv)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error converting pv to unstructured")
	}
	item = &unstructured.Unstructured{Object: m}
	return item, nil, nil
}

func (bta *BackupTimeAction) getZoneFromNodeAffinity(spec corev1api.PersistentVolumeSpec) string {
	nodeAffinity := spec.NodeAffinity
	if nodeAffinity == nil {
		return ""
	}
	for _, term := range nodeAffinity.Required.NodeSelectorTerms {
		if term.MatchExpressions == nil {
			continue
		}
		for _, exp := range term.MatchExpressions {
			if exp.Key == zoneTopologyKey && exp.Operator == "In" && len(exp.Values) > 0 {
				return exp.Values[0]
			}
		}
	}
	return ""
}
