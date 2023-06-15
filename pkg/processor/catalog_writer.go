/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package processor

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/operator-framework/catalogd/api/core/v1alpha1"
	"github.com/operator-framework/catalogd/internal/source"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type CatalogWriter interface {
	Write(context.Context, fs.FS, *v1alpha1.Catalog) error
}

type KubeWriter struct {
	client.Client
}

type FileCatalogWriter struct {
	// add fields related to populating catalog metadata and writing the catalog metadata to local files for off cluster
}

type CatalogProcessor struct {
	Unpacker source.Unpacker
	Writer   CatalogWriter
}

func (c *CatalogProcessor) Process(ctx context.Context, catalog *v1alpha1.Catalog) error {
	unpackResult, err := c.Unpacker.Unpack(ctx, catalog)
	if err != nil {
		return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("source bundle content: %v", err))
	}

	switch unpackResult.State {
	case source.StatePending:
		updateStatusUnpackPending(&catalog.Status, unpackResult)
		return nil
	case source.StateUnpacking:
		updateStatusUnpacking(&catalog.Status, unpackResult)
		return nil
	case source.StateUnpacked:
		// TODO: We should check to see if the unpacked result has the same content
		//   as the already unpacked content. If it does, we should skip this rest
		//   of the unpacking steps.

		err := c.Writer.Write(ctx, unpackResult.FS, catalog)
		if err != nil {
			return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("write catalog metadata to kube APIs: %v", err))
		}

		updateStatusUnpacked(&catalog.Status, unpackResult)
		return nil
	default:
		return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("unknown unpack state %q: %v", unpackResult.State, err))
	}
}

func (k *KubeWriter) Write(ctx context.Context, fsys fs.FS, catalog *v1alpha1.Catalog) error {
	fbc, err := declcfg.LoadFS(fsys)
	if err != nil {
		return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("load FBC from filesystem: %v", err))
	}

	if err := k.syncPackages(ctx, fbc, catalog); err != nil {
		return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("create package objects: %v", err))
	}

	if err := k.syncBundleMetadata(ctx, fbc, catalog); err != nil {
		return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("create bundle metadata objects: %v", err))
	}

	if err = k.syncCatalogMetadata(ctx, fsys, catalog); err != nil {
		return updateStatusUnpackFailing(&catalog.Status, fmt.Errorf("create catalog metadata objects: %v", err))
	}

	return nil
}

func updateStatusUnpackPending(status *v1alpha1.CatalogStatus, result *source.Result) {
	status.ResolvedSource = nil
	status.Phase = v1alpha1.PhasePending
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:    v1alpha1.TypeUnpacked,
		Status:  metav1.ConditionFalse,
		Reason:  v1alpha1.ReasonUnpackPending,
		Message: result.Message,
	})
}

func updateStatusUnpacking(status *v1alpha1.CatalogStatus, result *source.Result) {
	status.ResolvedSource = nil
	status.Phase = v1alpha1.PhaseUnpacking
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:    v1alpha1.TypeUnpacked,
		Status:  metav1.ConditionFalse,
		Reason:  v1alpha1.ReasonUnpacking,
		Message: result.Message,
	})
}

func updateStatusUnpacked(status *v1alpha1.CatalogStatus, result *source.Result) {
	status.ResolvedSource = result.ResolvedSource
	status.Phase = v1alpha1.PhaseUnpacked
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:    v1alpha1.TypeUnpacked,
		Status:  metav1.ConditionTrue,
		Reason:  v1alpha1.ReasonUnpackSuccessful,
		Message: result.Message,
	})
}

func updateStatusUnpackFailing(status *v1alpha1.CatalogStatus, err error) error {
	status.ResolvedSource = nil
	status.Phase = v1alpha1.PhaseFailing
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:    v1alpha1.TypeUnpacked,
		Status:  metav1.ConditionFalse,
		Reason:  v1alpha1.ReasonUnpackFailed,
		Message: err.Error(),
	})
	return err
}

// syncBundleMetadata will create a `BundleMetadata` resource for each
// "olm.bundle" object that exists for the given catalog contents. Returns an
// error if any are encountered.
func (k *KubeWriter) syncBundleMetadata(ctx context.Context, declCfg *declcfg.DeclarativeConfig, catalog *v1alpha1.Catalog) error {
	newBundles := map[string]*v1alpha1.BundleMetadata{}

	for _, bundle := range declCfg.Bundles {
		bundleName := fmt.Sprintf("%s-%s", catalog.Name, bundle.Name)

		bundleMeta := v1alpha1.BundleMetadata{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "BundleMetadata",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: bundleName,
				Labels: map[string]string{
					"catalog": catalog.Name,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         v1alpha1.GroupVersion.String(),
					Kind:               "Catalog",
					Name:               catalog.Name,
					UID:                catalog.UID,
					BlockOwnerDeletion: pointer.Bool(true),
					Controller:         pointer.Bool(true),
				}},
			},
			Spec: v1alpha1.BundleMetadataSpec{
				Catalog: corev1.LocalObjectReference{Name: catalog.Name},
				Package: bundle.Package,
				Image:   bundle.Image,
			},
		}

		for _, relatedImage := range bundle.RelatedImages {
			bundleMeta.Spec.RelatedImages = append(bundleMeta.Spec.RelatedImages, v1alpha1.RelatedImage{
				Name:  relatedImage.Name,
				Image: relatedImage.Image,
			})
		}

		for _, prop := range bundle.Properties {
			// skip any properties that are of type `olm.bundle.object`
			if prop.Type == "olm.bundle.object" {
				continue
			}

			bundleMeta.Spec.Properties = append(bundleMeta.Spec.Properties, v1alpha1.Property{
				Type:  prop.Type,
				Value: prop.Value,
			})
		}
		newBundles[bundleName] = &bundleMeta
	}

	var existingBundles v1alpha1.BundleMetadataList
	if err := k.List(ctx, &existingBundles); err != nil {
		return fmt.Errorf("list existing bundle metadatas: %v", err)
	}
	for _, existingBundle := range existingBundles.Items {
		if _, ok := newBundles[existingBundle.Name]; !ok {
			if err := k.Delete(ctx, &existingBundle); err != nil {
				return fmt.Errorf("delete existing bundle metadata %q: %v", existingBundle.Name, err)
			}
		}
	}

	ordered := sets.List(sets.KeySet(newBundles))
	for _, bundleName := range ordered {
		newBundle := newBundles[bundleName]
		if err := k.Client.Patch(ctx, newBundle, client.Apply, &client.PatchOptions{Force: pointer.Bool(true), FieldManager: "catalog-controller"}); err != nil {
			return fmt.Errorf("applying bundle metadata %q: %w", newBundle.Name, err)
		}
	}
	return nil
}

// syncPackages will create a `Package` resource for each
// "olm.package" object that exists for the given catalog contents.
// `Package.Spec.Channels` is populated by filtering all "olm.channel" objects
// where the "packageName" == `Package.Name`. Returns an error if any are encountered.
func (k *KubeWriter) syncPackages(ctx context.Context, declCfg *declcfg.DeclarativeConfig, catalog *v1alpha1.Catalog) error {
	newPkgs := map[string]*v1alpha1.Package{}

	for _, pkg := range declCfg.Packages {
		name := fmt.Sprintf("%s-%s", catalog.Name, pkg.Name)
		var icon *v1alpha1.Icon
		if pkg.Icon != nil {
			icon = &v1alpha1.Icon{
				Data:      pkg.Icon.Data,
				MediaType: pkg.Icon.MediaType,
			}
		}
		newPkgs[name] = &v1alpha1.Package{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "Package",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					"catalog": catalog.Name,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         v1alpha1.GroupVersion.String(),
					Kind:               "Catalog",
					Name:               catalog.Name,
					UID:                catalog.UID,
					BlockOwnerDeletion: pointer.Bool(true),
					Controller:         pointer.Bool(true),
				}},
			},
			Spec: v1alpha1.PackageSpec{
				Catalog:        corev1.LocalObjectReference{Name: catalog.Name},
				Name:           pkg.Name,
				DefaultChannel: pkg.DefaultChannel,
				Description:    pkg.Description,
				Icon:           icon,
				Channels:       []v1alpha1.PackageChannel{},
			},
		}
	}

	for _, ch := range declCfg.Channels {
		pkgName := fmt.Sprintf("%s-%s", catalog.Name, ch.Package)
		pkg, ok := newPkgs[pkgName]
		if !ok {
			return fmt.Errorf("channel %q references package %q which does not exist", ch.Name, ch.Package)
		}
		pkgChannel := v1alpha1.PackageChannel{Name: ch.Name}
		for _, entry := range ch.Entries {
			pkgChannel.Entries = append(pkgChannel.Entries, v1alpha1.ChannelEntry{
				Name:      entry.Name,
				Replaces:  entry.Replaces,
				Skips:     entry.Skips,
				SkipRange: entry.SkipRange,
			})
		}
		pkg.Spec.Channels = append(pkg.Spec.Channels, pkgChannel)
	}

	var existingPkgs v1alpha1.PackageList
	if err := k.List(ctx, &existingPkgs); err != nil {
		return fmt.Errorf("list existing packages: %v", err)
	}
	for _, existingPkg := range existingPkgs.Items {
		if _, ok := newPkgs[existingPkg.Name]; !ok {
			// delete existing package
			if err := k.Delete(ctx, &existingPkg); err != nil {
				return fmt.Errorf("delete existing package %q: %v", existingPkg.Name, err)
			}
		}
	}

	ordered := sets.List(sets.KeySet(newPkgs))
	for _, pkgName := range ordered {
		newPkg := newPkgs[pkgName]
		if err := k.Client.Patch(ctx, newPkg, client.Apply, &client.PatchOptions{Force: pointer.Bool(true), FieldManager: "catalog-controller"}); err != nil {
			return fmt.Errorf("applying package %q: %w", newPkg.Name, err)
		}
	}
	return nil
}

// syncCatalogMetadata will sync all of the catalog contents to `CatalogMetadata` objects
// by creating, updating and deleting the objects as necessary. Returns an
// error if any are encountered.

func (k *KubeWriter) syncCatalogMetadata(ctx context.Context, fsys fs.FS, catalog *v1alpha1.Catalog) error {
	newCatalogMetadataObjs := map[string]*v1alpha1.CatalogMetadata{}

	err := declcfg.WalkMetasFS(fsys, func(path string, meta *declcfg.Meta, err error) error {
		if err != nil {
			return fmt.Errorf("error in parsing catalog content files in the filesystem: %w", err)
		}

		packageOrName := meta.Package
		if packageOrName == "" {
			packageOrName = meta.Name
		}

		objName := generateCatalogMetadataName(ctx, catalog.Name, meta)

		catalogMetadata := &v1alpha1.CatalogMetadata{
			TypeMeta: metav1.TypeMeta{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "CatalogMetadata",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: objName,
				Labels: map[string]string{
					"catalog":       catalog.Name,
					"schema":        meta.Schema,
					"package":       meta.Package,
					"name":          meta.Name,
					"packageOrName": packageOrName,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         v1alpha1.GroupVersion.String(),
					Kind:               "Catalog",
					Name:               catalog.Name,
					UID:                catalog.UID,
					BlockOwnerDeletion: pointer.Bool(true),
					Controller:         pointer.Bool(true),
				}},
			},
			Spec: v1alpha1.CatalogMetadataSpec{
				Catalog: corev1.LocalObjectReference{Name: catalog.Name},
				Name:    meta.Name,
				Schema:  meta.Schema,
				Package: meta.Package,
				Content: meta.Blob,
			},
		}

		newCatalogMetadataObjs[catalogMetadata.Name] = catalogMetadata

		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to parse declarative config into CatalogMetadata API: %w", err)
	}

	var existingCatalogMetadataObjs v1alpha1.CatalogMetadataList
	if err := k.List(ctx, &existingCatalogMetadataObjs); err != nil {
		return fmt.Errorf("list existing catalog metadata: %v", err)
	}
	for _, existingCatalogMetadata := range existingCatalogMetadataObjs.Items {
		if _, ok := newCatalogMetadataObjs[existingCatalogMetadata.Name]; !ok {
			// delete existing catalog metadata
			if err := k.Delete(ctx, &existingCatalogMetadata); err != nil {
				return fmt.Errorf("delete existing catalog metadata %q: %v", existingCatalogMetadata.Name, err)
			}
		}
	}

	ordered := sets.List(sets.KeySet(newCatalogMetadataObjs))
	for _, catalogMetadataName := range ordered {
		newcatalogMetadata := newCatalogMetadataObjs[catalogMetadataName]
		if err := k.Client.Patch(ctx, newcatalogMetadata, client.Apply, &client.PatchOptions{Force: pointer.Bool(true), FieldManager: "catalog-controller"}); err != nil {
			return fmt.Errorf("applying catalog metadata %q: %w", newcatalogMetadata.Name, err)
		}
	}

	return nil
}

func generateCatalogMetadataName(ctx context.Context, catalogName string, meta *declcfg.Meta) string {
	objName := fmt.Sprintf("%s-%s", catalogName, meta.Schema)
	if meta.Package != "" {
		objName = fmt.Sprintf("%s-%s", objName, meta.Package)
	}
	if meta.Name != "" {
		objName = fmt.Sprintf("%s-%s", objName, meta.Name)
	}
	return objName
}
