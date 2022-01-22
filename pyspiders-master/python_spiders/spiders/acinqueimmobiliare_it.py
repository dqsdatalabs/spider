# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader 
import json

class MySpider(Spider):
    name = 'acinqueimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Acinqueimmobiliare_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=bilocale&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Cerca",
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=tre%20vani&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Aramak",
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=quattro%20vani&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Cerca",
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=cinque%20vani&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Aramak",
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=sette%20vani%20e%20oltre&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Aramak",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=loft&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Cerca",
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=villa%20o%20terratetto&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Aramak",
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=colonica&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Aramak",

                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://acinqueimmobiliare.it/immobili/?search_id_single&search_keyword&search_location=-1&search_type_single=monolocale&search_bedrooms=-1&search_bathrooms=-1&search_area_min&search_area_max&search_price_min&search_price_max&search_giar=-1&search_risk=0&search_balc=-1&search_ascen=-1&search_bpauto=-1&search_status=rent&property_search=Cerca",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Dettagli')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//div[@class='singlePropertyId']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        rent=response.xpath("//div[@class='sc_property_info_box_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(" ",""))
        item_loader.add_value("currency","EUR")
        address=response.xpath("//span[@class='contact_label contact_address_1']/text()").get()
        if address:
            item_loader.add_value("address",address)
        city=response.xpath("//span[@class='contact_label contact_address_1']/em/text()").get()
        if city:
            item_loader.add_value("city",city)
        square_meters=response.xpath("//div[@class='sc_property_info_item_area']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("Mq.")[0].strip())
        room_count=response.xpath("//span[@class='bedrooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("camer")[0])
        bathroom_count=response.xpath("//span[@class='bathrooms']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("serv")[0])
        energy_label=response.xpath("//span[@class='ape']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("energ.")[-1].split("-")[0].strip())
        desc=response.xpath("//div/p/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        parking=response.xpath("//li[contains(.,'Posto auto')]").get()
        if parking:
            item_loader.add_value("parking",True)
        elevator=response.xpath("//li[contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        floor=response.xpath("//li[contains(.,'Piano')]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.split(" ")[-1])
        images=[x.replace("-924x520","").replace("-565x520","").replace("-693x520","").replace("-926x520","").replace("-60x60","").replace("-783x520","").replace("-390x520","").replace("-926x520","").replace("-580x520","") for x in response.xpath("//img[@class='wp-post-image']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        terrace=response.xpath("//li[contains(.,'Terrazzo')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        balcony=response.xpath("//li[contains(.,'Balcone')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        item_loader.add_value("landlord_name","Acinque Immobiliare")
        item_loader.add_value("landlord_phone","055/609520")
        item_loader.add_value("landlord_email","info@acinqueimmobiliare.it")
        

        yield item_loader.load_item()