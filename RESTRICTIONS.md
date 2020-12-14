# Restrictions

To avoid high costs on the cloud platform, the pieline has the following restrictions.

* Identifier paths are only continuos inside Province Codes (cod_depto). If the identifier leaves this regions, the path will be interupted and will start again in the new Province.

* If the polygon is defined by a fixed group of identifiers, over a certain period of time. The pipeline will only construct future graphs looking inside the Province Codes detected during the timespan that defines the polygon 