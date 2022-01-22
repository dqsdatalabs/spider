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
import re

class MySpider(Spider):
    name = 'liveinit_com'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Liveinit_PySpider_italy"
    post_urls = ['http://liveinit.com/elenco-immobili.php']  # LEVEL 1
    
    formdata = {
        "externalSearch": "0",
        "id_tipo_offerta": "1",
        "id_tipologia": "20",
        "id_regione": "",
        "id_provincia": "",
        "id_comune": "",
        "zoneIDX": "",
        "prezzo": "0",
        "locali_min":"",
    }

    def start_requests(self):
        start_urls = [
            {
                "type": [
                    "20"
                ], 
                "property_type": "apartment"
            },
	        {
                "type": [
                    "21", "29"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.formdata["id_tipologia"] = item
                yield FormRequest(
                    url=self.post_urls[0],
                    dont_filter=True,
                    formdata=self.formdata,
                    callback=self.parse,
                    meta={
                        'property_type': url.get('property_type'),
                        "type": item
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h4/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url, 
                callback=self.populate_item, 
                meta={
                    "property_type": response.meta.get('property_type'), 
                    "type": response.meta.get('type')
                }
            )
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "externalSearch": "0",
                "order": "",
                "page": f"{page}",
                "displayResult": "",
                "id_tipo_offerta": "1",
                "id_tipologia": response.meta.get('type'),
                "id_regione": "",
                "id_provincia": "",
                "id_comune": "",
                "zoneIDX": "",
                "prezzo": "0",
                "locali_min": "",
            }
            yield FormRequest(
                url=self.post_urls[0], 
                callback=self.parse, 
                formdata=formdata,
                dont_filter=True,
                meta={
                    "page": page+1, 
                    "property_type": response.meta.get('property_type'),
                    "type": response.meta.get('type')
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//h4/span[@class='label price']/i/following-sibling::text()").get()
        if title:
            item_loader.add_value("title",title.replace("\r\n","").replace("  ",""))
        external_id=response.xpath("//strong[.='Codice:']/following-sibling::strong/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=item_loader.get_output_value("title")
        if rent:
            rent=rent.split("-")[-1].split(",")[0].replace(".","")
            rent=re.findall("\d+",rent)
            if rent:
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters=response.xpath("//strong[contains(.,'Superficie')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        room_count=response.xpath("//strong[contains(.,'Locali')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//strong[contains(.,'Bagni')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        city = "".join(response.xpath("//span[@class='label price']/text()").getall())
        if city:
            item_loader.add_value("city", city.split('-')[0].strip())
            item_loader.add_value("address", city.split('-')[0].strip())

        latlng = "".join(response.xpath("//script[@type='text/javascript']/text()[contains(.,'var lat')]").extract())
        if latlng:
            latitude = latlng.split('lat = ')[-1].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            longitude = latlng.split('lng = ')[-1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)

        desc=" ".join(response.xpath("//h2[.='Descrizione']/following-sibling::p/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//div[@class='item']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        energy_label=response.xpath("//strong[contains(.,'Classe energetica')]/following-sibling::img/@alt").extract()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        item_loader.add_value("landlord_name","LIVE INIT")
        item_loader.add_value("landlord_email","info@liveinit.com")
        item_loader.add_value("landlord_phone","+39 06 68135740")
        yield item_loader.load_item()