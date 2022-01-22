# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import re

class MySpider(Spider):
    name = 'agence-chene_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source="AgenceChene_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agence-chene.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agence-chene.com/catalog/advanced_search_result.php?action=update_search&search_id=1721107974754685&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='panel-img']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        nextpage=response.xpath("//li/a[@aria-label='Next']/@href").get()
        if nextpage:
            nextpage="https://www.agence-chene.com/"+nextpage
            yield Request(nextpage, callback=self.parse, meta={"property_type":response.meta["property_type"]})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        
        external_id=response.xpath("//span[contains(.,'Ref.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        adres=response.xpath("//div[@class='bienCity']/h2/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("Loyer")[-1].split("€")[0].strip().replace(" ","").split(".")[0].replace("\xa0",""))
        item_loader.add_value("currency","EUR")
        description=response.xpath("//div[@class='col-xs-12 col-sm-7 col-md-7 col-lg-7 infoDesc']/p//text()").getall()
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//span[contains(.,'pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        if not room_count:
            room=response.xpath("//h1[@class='entry-title page-header']/text()").get()
            if "studio" in room.lower():
                item_loader.add_value("room_count","1")
            if "pièce" in room:
                item_loader.add_value("room_count",room.split("pièce")[0].strip().split(" ")[-1])
            if "chambres" in room:
                item_loader.add_value("room_count",room.split("chambres")[0].strip().split(" ")[-1])
        adres=response.xpath("//span[@class='alur_location_ville']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=adres.split(" ")[1]
        if city:
            item_loader.add_value("city",city)
        zipcode=adres.split(" ")[0]
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        square_meters=response.xpath("//span[@class='alur_location_surface']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split("m²")[0].split(".")[0])
        deposit=response.xpath("//span[@class='alur_location_depot']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("€")[0].replace(" ","").split(".")[0])
        images=[x for x in response.xpath("//div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        utilities=response.xpath("//span[@class='alur_location_honos']/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].replace(" ","").split(".")[0])
        latitude=response.xpath("//script[contains(.,' google.maps.LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("myOptions")[0].split("google.maps.LatLng")[-1].split(",")[0].replace("(",""))
        longitude=response.xpath("//script[contains(.,' google.maps.LatLng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("myOptions")[0].split("google.maps.LatLng")[-1].split(",")[-1].split(");")[0])
        item_loader.add_value("landlord_name","Agence Le Grand Chêne ")
        yield item_loader.load_item()