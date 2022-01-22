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
    name = 'silvanamenetti_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Silvanamenetti_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.silvanamenetti.it/risultati.php?tipologia=appartamento&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica="
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.silvanamenetti.it/risultati.php?tipologia=attico&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica=",
                    "https://www.silvanamenetti.it/risultati.php?tipologia=villa&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica=",
                    "https://www.silvanamenetti.it/risultati.php?tipologia=casa&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica=",
                    "https://www.silvanamenetti.it/risultati.php?tipologia=rustico&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica=",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.silvanamenetti.it/risultati.php?tipologia=monolocale&tipomediaz=locazione&zona=&prezzo=&prezzo2a=&prezzo2b=&npratica=",
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

        for item in response.xpath("//div[@id='guarda-scheda']/@onclick").extract():
            follow_url = response.urljoin(item.split("'")[1])
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_check = "".join(response.xpath("//p[@id='annuncio-scheda']//text()").getall())
        if prop_check and "monolocale" in prop_check.lower():
            property_type = "studio"
        else:
            property_type = response.meta.get('property_type')

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Riferimento:')]//following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u2013",""))

        rent=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Prezzo:')]//following-sibling::text()").get()
        if   "trat" not in rent:
            item_loader.add_value("rent",rent)
        else:
            return
        item_loader.add_value("currency","EUR")

        square_meters=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Metri Quadri:')]//following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        city = response.xpath("//span[contains(.,'Appartamento')]/following-sibling::span[1]/a/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        else:
            city = response.xpath("//span[contains(.,'Monolocale')]/following-sibling::span[1]/a/text()").get()
            if city:
                item_loader.add_value("city", city.strip())
        
        address=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Zona:')]//following-sibling::text()").get()
        if address:
            item_loader.add_value("address",address)

        if property_type == "studio":
            item_loader.add_value("room_count", 1)
        else:
            room_count=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Camere:')]//following-sibling::text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count)

        bathroom_count=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Bagni:')]//following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        
        floor=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Piano:')]//following-sibling::text()").get()
        if floor:
            item_loader.add_value("floor",floor.split('°')[0].strip())

        description=response.xpath("//p[@id='annuncio-scheda']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        elevator=response.xpath("//ul[@class='caratteristiche']//li//span[contains(.,'Ascensore:')]//following-sibling::text()").get()
        if elevator and "ascensore" in elevator.lower():
            item_loader.add_value("elevator", True)
        elif elevator and "si" in elevator.lower():
            item_loader.add_value("elevator", True) 
        elif elevator and "1" in elevator.lower():
            item_loader.add_value("elevator", True)

        terrace=response.xpath("//span[contains(.,'Terrazzi')]/following-sibling::text()").get()
        if terrace and "terrazzi" in terrace.lower():
            item_loader.add_value("terrace", True)
        elif terrace and "si" in terrace.lower():
            item_loader.add_value("terrace", True) 
        elif terrace and "1" in terrace.lower():
            item_loader.add_value("terrace", True) 

        parking=response.xpath("//span[contains(.,'Parcheggi')]/following-sibling::text()").get()
        if parking and "garage" in elevator.lower():
            item_loader.add_value("parking", True)
        elif parking and "si" in elevator.lower():
            item_loader.add_value("parking", True) 
        elif parking and "1" in parking.lower():
            item_loader.add_value("parking", True) 

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@id,'galleria')]//img//@src").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "051 6449663")
        item_loader.add_value("landlord_email", "immobiliare@silvanamenetti.it")
        item_loader.add_value("landlord_name", "Silvana Menetti Immobiliare")

    

        yield item_loader.load_item()