# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin

class MySpider(Spider):
    name = 'ajoaimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://ajoaimmobilier.fr/location-immo-aulnaysousbois.php",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='property-thumb-info-content']//a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Ajoaimmobilier_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title","//h2/span//text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id=")[1])
        
        rent = response.xpath("//h2/span//text()[2]").get()
        if rent:
            item_loader.add_value("rent_string", rent.split('-')[-1].replace(' ', ''))
        
        square_meters="".join(response.xpath("//div[contains(@class,'bloc-detail')]/strong[contains(.,'Surface')]//parent::div/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count=response.xpath("//ul/li/i[contains(@class,'bedroom')]//parent::li/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        city="".join(response.xpath("//div[contains(@class,'bloc-detail')]/strong[contains(.,'Ville')]//parent::div/text()").getall())
        district="".join(response.xpath("//div[contains(@class,'bloc-detail')]/strong[contains(.,'Quartier')]//parent::div/text()").getall())
        if district or city:
            item_loader.add_value("address", city.strip()+" "+district.strip())
            item_loader.add_value("city", city.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'Latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('initLatitude=')[1].split(';')[0]
            longitude = latitude_longitude.split('initLongitude=')[1].split(';')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            

        desc="".join(response.xpath("//div[@class='col-sm-8']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip().replace("\n","").replace("\t",""))
        
        deposit = response.xpath("//div[@class='col-sm-8']//text()[contains(.,'de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        images=[x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        floor="".join(response.xpath("//div[contains(@class,'bloc-detail')]/strong[contains(.,'Etage')]//parent::div/text()").getall())
        if floor:
            item_loader.add_value("floor", floor.strip())
        parking="".join(response.xpath("//div[contains(@class,'bloc-detail')]/strong[contains(.,'Parking')]//parent::div/text()").getall())
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        terrace="".join(response.xpath("//div[contains(@class,'bloc-detail')]/strong[contains(.,'Terrasse')]//parent::div/text()").getall())
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_phone", '01 84 81 11 00')
        item_loader.add_value("landlord_email", 'contact@ajoaimmobilier.fr')
        item_loader.add_value("landlord_name", 'Alexandra QUIGNON')
        
        yield item_loader.load_item()
