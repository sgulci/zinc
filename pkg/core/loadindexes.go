/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

package core

import (
	"fmt"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog/log"

	"github.com/zinclabs/zinc/pkg/errors"
	"github.com/zinclabs/zinc/pkg/metadata"
	zincanalysis "github.com/zinclabs/zinc/pkg/uquery/analysis"
)

func LoadZincIndexesFromMetadata() error {
	indexes, err := metadata.Index.List(0, 0)
	if err != nil {
		return err
	}

	for i := range indexes {
		// cache mappings
		index := new(Index)
		index.Name = indexes[i].Name
		index.StorageType = indexes[i].StorageType
		index.StorageSize = indexes[i].StorageSize
		index.DocNum = indexes[i].DocNum
		index.ShardNum = indexes[i].ShardNum
		index.Shards = append(index.Shards, indexes[i].Shards...)
		index.Settings = indexes[i].Settings
		index.Mappings = indexes[i].Mappings
		index.CreateAt = indexes[i].CreateAt
		index.UpdateAt = indexes[i].UpdateAt
		log.Info().Msgf("Loading index... [%s:%s] shards[%d]", index.Name, index.StorageType, index.ShardNum)

		// load index analysis
		if index.Settings != nil && index.Settings.Analysis != nil {
			index.Analyzers, err = zincanalysis.RequestAnalyzer(index.Settings.Analysis)
			if err != nil {
				return errors.New(errors.ErrorTypeRuntimeException, "parse stored analysis error").Cause(err)
			}
		}
		// load in memory
		ZINC_INDEX_LIST.Add(index)
	}

	return nil
}

func (index *Index) GetWriter() (*bluge.Writer, error) {
	index.lock.RLock()
	w := index.Shards[index.ShardNum-1].Writer
	index.lock.RUnlock()
	if w != nil {
		return w, nil
	}

	// open writer
	if err := index.openWriter(index.ShardNum - 1); err != nil {
		return nil, err
	}

	// update metadata
	index.UpdateMetadata()

	index.lock.RLock()
	w = index.Shards[index.ShardNum-1].Writer
	index.lock.RUnlock()

	return w, nil
}

func (index *Index) GetReader() (*bluge.Reader, error) {
	w, err := index.GetWriter()
	if err != nil {
		return nil, err
	}
	return w.Reader()
}

func (index *Index) openWriter(shard int) error {
	var defaultSearchAnalyzer *analysis.Analyzer
	if index.Analyzers != nil {
		defaultSearchAnalyzer = index.Analyzers["default"]
	}

	indexName := fmt.Sprintf("%s/%06x", index.Name, shard)
	var err error
	index.lock.Lock()
	index.Shards[shard].Writer, err = OpenIndexWriter(indexName, index.StorageType, defaultSearchAnalyzer, 0, 0)
	index.lock.Unlock()
	return err
}
