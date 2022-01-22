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
    name = 'addictimmobilier31_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Addictimmobilier31_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.addictimmobilier31.com/ajax/ListeBien.php?page=1&TypeModeListeForm=text&tdp=5&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.Map.Liste&Pagination=0",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.addictimmobilier31.com/ajax/ListeBien.php?page=1&TypeModeListeForm=text&tdp=5&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.Map.Liste&Pagination=0"
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
        
        for item in response.xpath("//a[contains(@class,'simple-btn')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title.replace("\u00e8","").replace("\u00b2",""))

        external_id=response.xpath("//span[@itemprop='productID']//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)   

        description=response.xpath("//span[contains(@itemprop,'description')]//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        city=response.xpath("//h2[contains(@class,'detail-bien-ville')]//text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[0])

        zipcode=response.xpath("//h2[contains(@class,'detail-bien-ville')]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].split(")")[0])

        room_count=response.xpath("//li[contains(.,'pièce')]//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("pièce(s)"))
        else:
            room_count=response.xpath("//li[contains(.,'chambre')]//text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count.split("chambre(s)"))

        square_meters=response.xpath("//li[contains(.,'m²')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip()
            item_loader.add_value("square_meters",square_meters)

        rent=response.xpath("//div[contains(@class,'detail-bien-prix')]//text()[contains(.,'€ par mois')]").get()
        if  rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        utilities=response.xpath("//ul[contains(@class,'nolist')]//li//span[contains(@class,'cout_charges_mens')]//text()").get()
        if  utilities:
            item_loader.add_value("utilities",utilities)

        deposit=response.xpath("//ul[contains(@class,'nolist')]//li//span[contains(.,'Dépôt de garantie')]//following-sibling::span//text()").get()
        if  deposit:
            item_loader.add_value("deposit",deposit)

        images=[response.urljoin(x) for x in response.xpath("//div[@class='is-flap']//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)

        latitude = response.xpath("//ul[contains(@class,'gg-map-marker hidden')]//li[contains(@class,'gg-map-marker-lat')]//text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)  

        longitude = response.xpath("//ul[contains(@class,'gg-map-marker hidden')]//li[contains(@class,'gg-map-marker-lng')]//text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)  
 
        item_loader.add_value("landlord_phone", "05.63.33.52.48")
        item_loader.add_value("landlord_email", "ADDICT-IMMOBILIER31@ORANGE.FR")
        item_loader.add_value("landlord_name", "Addict immobilier 31")


        yield item_loader.load_item()