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
    name = 'immoexpert_fr'
    external_source = "Immoexpert_PySpider_france_fr"
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings= {
        "PROXY_FR_ON" : True
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immoexpert.fr/en/listing-location.html?loc=location&type%5B%5D=appartement&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=&page=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immoexpert.fr/en/listing-location.html?loc=location&type%5B%5D=maison&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=prix-asc&page=1"
                ],
                "property_type": "house"
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
        
        page = response.meta.get('page', 2)
        property_type = response.meta.get("property_type")
        seen = False
        for item in response.xpath("//div[@class='btn btn-primary btn-sm']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        if property_type and "apartment" in property_type.lower():
            if page == 2 or seen:
                url = f"https://www.immoexpert.fr/en/listing-location.html?loc=location&type%5B%5D=appartement&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=prix-asc&page={page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
        else:
            if page == 2 or seen:
                url = f"https://www.immoexpert.fr/en/listing-location.html?loc=location&type%5B%5D=maison&surfacemin=&prixmax=&numero=&coordonnees=&archivage_statut=0&tri=prix-asc&page={page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title) 

        external_id = response.xpath("//li[@class='c_numero']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        city = response.xpath("//li[@class='c_ville']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if city:
            item_loader.add_value("city",city)
            item_loader.add_value("address",city)

        rent = response.xpath("//li[@class='c_prix']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if rent:
            rent = rent.split("€")[1]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        square_meters = response.xpath("//li[@class='c_surface']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

        floor = response.xpath("//li[@class='c_etages']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if floor:
            item_loader.add_value("floor",floor) 
            
        bathroom_count = response.xpath("//li[@class='c_sbain']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        room_count = response.xpath("//li[@class='c_piece']//span[contains(@class ,'attribut_label0')]//parent::span//following-sibling::span[@class='bloc-champ']//span[@class='champ']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count) 

        images = [x for x in response.xpath("//div[contains(@class,'carousel-inner')]//div[contains(@class,'carousel-item')]//img//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value("landlord_name", "Immoexpert")
        item_loader.add_value("landlord_phone", "03 29 30 31 31")
          
        yield item_loader.load_item()