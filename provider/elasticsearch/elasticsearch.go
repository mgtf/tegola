package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	//"os"
	//"strings"

	"github.com/olivere/elastic"

	"github.com/terranodo/tegola"
	"github.com/terranodo/tegola/basic"
	"github.com/terranodo/tegola/mvt"
	"github.com/terranodo/tegola/mvt/provider"
	"github.com/terranodo/tegola/util/dict"
	//"github.com/terranodo/tegola/wkb"
)

type Job struct {
	Name         string `json:"user"`
	Location     string `json:"location"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	ContractType string `json:"contract_type"`
	Profession   string `json:"profession"`
	Category     string `json:"category"`
}

// layer holds information about a query.
type layer struct {
	// The Name of the layer
	Name string
	// The SQL to use. !BBOX! token will be replaced by the envelope
	SQL string
	// The ID field name, this will default to 'gid' if not set to something other then empty string.
	IDFieldName string
	// The Geometery field name, this will default to 'geom' if not set to soemthing other then empty string.
	GeomFieldName string
	// The SRID that the data in the table is stored in. This will default to WebMercator
	SRID int
}

// Provider provides the elasticsearch data provider.
type Provider struct {
	layers map[string]layer // map of layer name and corrosponding sql
	srid   int
}

// DEFAULT sql for get geometries,
const BBOX = "!BBOX!"

// We quote the field and table names to prevent colliding with postgres keywords.
const stdSQL = `SELECT %[1]v FROM %[2]v WHERE "%[3]v" && ` + BBOX

// SQL to get the column names, without hitting the information_schema. Though it might be better to hit the information_schema.
const fldsSQL = `SELECT * FROM %[1]v LIMIT 0;`

const Name = "elasticsearch"
const DefaultPort = 9200
const DefaultSRID = tegola.WebMercator
const DefaultMaxConn = 5

const (
	ConfigKeyHost        = "host"
	ConfigKeyPort        = "port"
	ConfigKeyDB          = "database"
	ConfigKeyUser        = "user"
	ConfigKeyPassword    = "password"
	ConfigKeyMaxConn     = "max_connection"
	ConfigKeySRID        = "srid"
	ConfigKeyLayers      = "layers"
	ConfigKeyLayerName   = "name"
	ConfigKeyTablename   = "tablename"
	ConfigKeySQL         = "sql"
	ConfigKeyFields      = "fields"
	ConfigKeyGeomField   = "geometry_fieldname"
	ConfigKeyGeomIDField = "id_fieldname"
)

func init() {
	provider.Register(Name, NewProvider)
}

func NewProvider(config map[string]interface{}) (mvt.Provider, error) {
	c := dict.M(config)
	p := Provider{}
	layers, ok := c[ConfigKeyLayers].([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected %v to be a []map[string]interface{}", ConfigKeyLayers)
	}

	lyrs := make(map[string]layer)
	lyrsSeen := make(map[string]int)

	for i, v := range layers {
		vc := dict.M(v)

		lname, err := vc.String(ConfigKeyLayerName, nil)
		if err != nil {
			return nil, fmt.Errorf("For layer(%v) we got the following error trying to get the layer's name field: %v", i, err)
		}
		if j, ok := lyrsSeen[lname]; ok {
			return nil, fmt.Errorf("%v layer name is duplicated in both layer %v and layer %v", lname, i, j)
		}
		lyrsSeen[lname] = i

		geomfld := "geom"
		geomfld, err = vc.String(ConfigKeyGeomField, &geomfld)
		if err != nil {
			return nil, fmt.Errorf("For layer(%v) %v : %v", i, lname, err)
		}
		idfld := "gid"
		idfld, err = vc.String(ConfigKeyGeomIDField, &idfld)
		if err != nil {
			return nil, fmt.Errorf("For layer(%v) %v : %v", i, lname, err)
		}
		if idfld == geomfld {
			return nil, fmt.Errorf("For layer(%v) %v: %v (%v) and %v field (%v) is the same!", i, lname, ConfigKeyGeomField, geomfld, ConfigKeyGeomIDField, idfld)
		}

		var tblName string
		tblName, err = vc.String(ConfigKeyTablename, &lname)
		if err != nil {
			return nil, fmt.Errorf("for %v layer(%v) %v has an error: %v", i, lname, ConfigKeyTablename, err)
		}
		var sql string

		sql, err = vc.String(ConfigKeySQL, &sql)

		if err != nil {
			return nil, fmt.Errorf("for %v layer(%v) %v has an error: %v", i, lname, ConfigKeySQL, err)
		}

		if tblName != lname && sql != "" {
			log.Printf("Both %v and %v field are specified for layer(%v) %v, using only %[2]v field.", ConfigKeyTablename, ConfigKeySQL, i, lname)
		}

		l := layer{
			Name:          lname,
			IDFieldName:   idfld,
			GeomFieldName: geomfld,
		}
		lyrs[lname] = l
	}
	p.layers = lyrs

	return p, nil
}

func (p Provider) LayerNames() (names []string) {
	for k, _ := range p.layers {
		names = append(names, k)
	}
	return names
}

func (p Provider) MVTLayer(layerName string, tile tegola.Tile, tags map[string]interface{}) (layer *mvt.Layer, err error) {

	plyr, ok := p.layers[layerName]
	if !ok {
		return nil, fmt.Errorf("Don't know of the layer %v", layerName)
	}

	textent := tile.BoundingBox()
	minGeo, err := basic.FromWebMercator(plyr.SRID, &basic.Point{textent.Minx, textent.Miny})
	if err != nil {
		return nil, fmt.Errorf("Got error trying to convert tile point. %v ", err)
	}
	maxGeo, err := basic.FromWebMercator(plyr.SRID, &basic.Point{textent.Maxx, textent.Maxy})
	if err != nil {
		return nil, fmt.Errorf("Got error trying to convert tile point. %v ", err)
	}
	minPt, ok := minGeo.(*basic.Point)
	if !ok {
		return nil, fmt.Errorf("Expected Point, got %t %v", minGeo)
	}
	maxPt, ok := maxGeo.(*basic.Point)
	if !ok {
		return nil, fmt.Errorf("Expected Point, got %t %v", maxGeo)
	}

	q := elastic.NewGeoBoundingBoxQuery("jobs")
	q.TopRight(maxPt.Y(), maxPt.X())
	q.BottomLeft(minPt.Y(), minPt.X())

	client, err := elastic.NewClient(
		elastic.SetURL("http://localhost:9200", "http://localhost:9201"),
		elastic.SetMaxRetries(10))
	//elastic.SetBasicAuth("user", "secret"))

	ctx := context.Background()

	searchResult, err := client.Search().
		Index("jobs"). // search in index "twitter"
		Query(q).      // specify the query
		Pretty(true).  // pretty print request and response JSON
		Do(ctx)        // execute
	if err != nil {
		// Handle error
		return nil, fmt.Errorf("Got the following error (%v) running query", err)
	}

	// var geobytes []byte

	layer = new(mvt.Layer)
	layer.Name = layerName
	var count int
	// var didEnd bool
	fmt.Printf("Query : %s\n", "Jobs")

	if searchResult.Hits.TotalHits > 0 {
		for _, hit := range searchResult.Hits.Hits {
			var j Job
			err := json.Unmarshal(*hit.Source, &j)
			if err != nil {
				// Deserialization failed
			}

			count++
			// var geom tegola.Geometry
			// var gid uint64

			// Work with tweet
			fmt.Printf("Job : %s\n", j.Name)
		}
	}

	return layer, nil
}
