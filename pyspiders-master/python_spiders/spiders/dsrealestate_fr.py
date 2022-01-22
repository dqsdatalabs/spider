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
    name = 'dsrealestate_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Dsrealestate_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.dsrealestate.fr/fr/location-meublee-appartement-maison-paris/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//article[@class='annonce_listing']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            prop_type = item.xpath(".//div[@class='descriptif']/ul/li[1]/text()").get()
            if "appartement" in prop_type:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "studio" in prop_type:
                property_type = "studio"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "maison" in prop_type or "duplex" in prop_type:
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            

        pagination = response.xpath("//div[@class='pagination']/a[@class='btn-go-next']/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Dsrealestate_PySpider_"+ self.country + "_" + self.locale)

        external_id=response.xpath(
            "//div[@class='property_infos']/ul/li/span[contains(.,'Référence')]//following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent=response.xpath("//span[@class='value']/text()").get()
        if rent:
            rent = rent.replace("\xa0","")
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))
        
        square_meters=response.xpath(
            "//div[@class='property_infos']/ul/li/span[contains(.,'Surface')]//following-sibling::span/text()").get()
        if "." in square_meters:
            item_loader.add_value("square_meters", square_meters.split('.')[0].strip())
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        
        room_count=response.xpath(
            "//div[@class='property_infos']/ul/li/span[contains(.,'pièces')]//following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        
        zipcode = response.xpath("//h2//span[@itemprop='postalCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//h2//span[@itemprop='addressLocality']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        address =" ".join(response.xpath("//h2/span[contains(.,'Quartier')]/following-sibling::span/text()").getall())
        address_city=" ".join(response.xpath("//h2/span/span/text()").getall())
        if address:
            address = address +", "+address_city
            item_loader.add_value("address", address.strip())
        elif address_city:
            item_loader.add_value("address", address_city.strip())

        desc="".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            if "Machine à laver" in desc:
                item_loader.add_value("washing_machine",True)
            
        
        images=[response.urljoin(x) for x in response.xpath("//div[@class='diapo']/div/figure/img[@class='auto--Scale']/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished =" ".join(response.xpath("//span[@class='honoraires']//text()[normalize-space()]").getall())
        if furnished:
            if "meublé" in furnished.lower():
                item_loader.add_value("furnished", True)
        parking = response.xpath(
            "//div[@class='property_infos']/ul/li/span[contains(.,'parking')]//following-sibling::span//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        deposit=response.xpath(
            "//ul[@class='infos_location']/li[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(':')[1].split('€')[0].replace(" ","").strip())
        
        item_loader.add_xpath("landlord_name","//li[@itemprop='legalName']/text()")
        phone=response.xpath("//span[@itemprop='telephone']/text()").get()
        item_loader.add_value("landlord_phone", phone)
        
        yield item_loader.load_item()