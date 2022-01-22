# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'statusimmobiliare_it'
    external_source = "Statusimmobiliare_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.statusimmobiliare.it/risultati.php?tipomediaz=affitto&provincia=&prezzomax=&mqmin=&via=&ascensore=&riscaldamento=&terrazzi=&garage=&codice=&zone=&tipopag=1&tipologie=*appartamento*",
                ],
                "property_type": "apartment",
                "type": "*appartamento*"
            }, 
	        {
                "url": [
                    "https://www.statusimmobiliare.it/risultati.php?tipomediaz=affitto&provincia=&prezzomax=&mqmin=&via=&ascensore=&riscaldamento=&terrazzi=&garage=&codice=&zone=&tipopag=1&tipologie=*attico**villa**casa*"
                ],
                "property_type": "house",
                "type": "*attico**villa**casa*"
                
            },
            {
                "url": [
                    "https://www.statusimmobiliare.it/risultati.php?tipomediaz=affitto&provincia=&prezzomax=&mqmin=&via=&ascensore=&riscaldamento=&terrazzi=&garage=&codice=&zone=&tipopag=1&tipologie=*monolocale*",
                ],
                "property_type": "studio",
                "type": "*monolocale*"
                
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), 'type': url.get('type')}
                )

    # 1. FOLLOWING
    def parse(self, response):        
        for item in response.xpath("//div[contains(@class,'immobile-colonna')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        total_page = response.xpath("//div[@class='paging2']//a/text()").getall()
        if total_page:
            for i in range(2,int(total_page[-1])+1):
                url = f"https://www.statusimmobiliare.it/risultati.php?tipomediaz=affitto&provincia=&prezzomax=&zone=&tipologie={response.meta.get('type')}&mq=&camere=&via=&ascensore=&riscaldamento=&terrazzi=&garage=&codice=&ordine=&pag={i}"
                yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type'), 'type': response.meta.get('type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//ul//li[contains(.,'prezzo:')]//following-sibling::text()").get()
        if dontallow and "riservata" in dontallow.lower():
            return 
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        external_id = "".join(response.xpath("//h2[contains(@class,'codice')]//text()").get())
        if external_id:
            external_id=external_id.split(":")[1]
            item_loader.add_value("external_id",external_id)

        address = response.xpath("//ul//li[contains(.,'zona:')]//following-sibling::text()").get()
        if address:
            item_loader.add_value("address",address)
        rent = response.xpath("//ul//li[contains(.,'prezzo:')]//following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters = response.xpath("//div[contains(@id,'mq')]//text()").get()
        if square_meters:
            square_meters=square_meters.split("Mq")
            item_loader.add_value("square_meters",square_meters)
        desc=response.xpath("//div[@class='dati-scheda']/p/text()").get()
        if desc:
            item_loader.add_value("description",desc)
        city=response.xpath("//div[@class='dati-scheda']/h1/text()").get()
        if city:
            item_loader.add_value("city",city.split("-")[-1].strip())

        bathroom_count = response.xpath("//ul//li[contains(.,'bagni:')]//following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        room_count = response.xpath("//ul//li[contains(.,'vani:')]//following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        energy_label = response.xpath("//ul//li[contains(.,'classe energetica:')]//following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        terrace = response.xpath("//div[contains(@id,'tipo')]//text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        else:
            item_loader.add_value("terrace",False)

        parking =response.xpath("//div[contains(@id,'garage')]//text()").get()
        if parking:
            item_loader.add_value("parking",True)
        else:
            item_loader.add_value("parking",False)

        images = [response.urljoin(x)for x in response.xpath("//img[contains(@id,'fotoaltreprop')]//@src").extract()]
        if images:
                item_loader.add_value("images",images)

        item_loader.add_value("landlord_phone", "051 392299")
        item_loader.add_value("landlord_email", "immobiliare@statusimmobiliare .it")
        item_loader.add_value("landlord_name", "Status Immobiliare")

        yield item_loader.load_item()