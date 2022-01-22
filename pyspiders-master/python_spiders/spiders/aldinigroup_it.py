# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'aldinigroup_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Aldinigroup_PySpider_italy"
    post_urls = ['https://www.aldinigroup.it/immobili/lista_immobili']  # LEVEL 1

    formdata = {
        "f_destinazione": "Affitto",
        "f_provincia": "0",
        "f_tipo": "1",
        "f_categoria_web": "0",
        "f_testo_like": "",
        "f_sup_dal": "",
        "f_sup_al": "",
        "f_vani_dal": "",
        "f_vani_al": "",
        "f_prezzo_dal": "",
        "f_prezzo_al": "",
        "f_codice": "",
        "sf-submit-1": "Cerca",
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    }
    
    def start_requests(self):
        
        start_urls = [
            {
                "type": [
                    "1",
                ],
                "property_type": "apartment"
            },
	        {
                "type": [
                    "4",
                    "10"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.formdata["f_tipo"] = item
                yield FormRequest(
                    url=self.post_urls[0],
                    dont_filter = True,
                    formdata = self.formdata,
                    callback=self.parse,
                    headers = self.headers,
                    meta={'property_type': url.get('property_type'), "type": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='panel']"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Scheda dettagliata')]/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:
            url = f"https://www.aldinigroup.it/immobili/lista_immobili/{page}"
            self.formdata["f_tipo"] = response.meta.get('type')
            yield FormRequest(
                url,
                dont_filter=True, 
                callback=self.parse, 
                formdata=self.formdata, 
                headers=self.headers, 
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
        
        item_loader.add_xpath("title", "//h4/text()")
        
        address = "".join(response.xpath("//div[strong[contains(.,'Indirizzo')]]/following-sibling::div/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split("-")[-1].split("(")[0].strip()
            item_loader.add_value("city", city)
        
        rent = response.xpath("//h4[contains(.,'€')]/text()").get()
        if rent:
            rent = rent.split(",")[0].split("€")[1].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        item_loader.add_xpath("external_id", "//div[strong[contains(.,'Codice')]]/following-sibling::div/text()")
        
        square_meters = response.xpath("//span[@class='ico-24-mq']/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//span[@class='ico-24-camere']/text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[strong[contains(.,'Bagni')]]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = "".join(response.xpath("//div[strong[contains(.,'Informazioni')]]/following-sibling::div/text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        elevator = response.xpath("//div[strong[contains(.,'Ascensore')]]/following-sibling::div/text()").get()
        if elevator:
            if "no" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "si" in elevator.lower():
                item_loader.add_value("elevator", True)
        parking=response.xpath("//strong[.='Box/Garage']/parent::div/following-sibling::div/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        
        furnished = response.xpath("//div[strong[contains(.,'Arredato')]]/following-sibling::div/text()").get()
        if furnished:
            if "no" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "si" in furnished.lower():
                item_loader.add_value("furnished", True)   
        
        swimming_pool = response.xpath("//div[strong[contains(.,'Piscina')]]/following-sibling::div/text()").get()
        if swimming_pool:
            if "no" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            elif "si" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)        
        
        energy_label = response.xpath("//div[strong[contains(.,'Classe energetica')]]/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        parking = response.xpath("//div[strong[contains(.,'Box/Garage')]]/following-sibling::div/text()").get()
        if parking:
            if "no" not in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        images = [x for x in response.xpath("//div[@class='carousel-inner']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat:")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split("{")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Aldini Group")
        item_loader.add_value("landlord_phone", "091586530")
        item_loader.add_value("landlord_email", "palermo@aldinire.com")
        
        yield item_loader.load_item()