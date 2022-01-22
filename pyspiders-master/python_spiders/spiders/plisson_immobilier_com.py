# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'plisson_immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    start_urls = ['https://www.plisson-immobilier.com/location-appartement-paris/&new_research=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article[@class='annonce_listing']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        page = response.xpath("//div[@class='pagination']/a[not(contains(.,'>'))][last()]/text()").get()
        for i in range(2,int(page)+1):
            url = f"https://www.plisson-immobilier.com/location-appartement-paris/p={i}"
            yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//span[@class='type']/text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type", "apartment")
        else: return
        item_loader.add_value("external_source", "PlissonImmobilier_PySpider_france")
        externalid= response.xpath("//div[@class='annonce_ref']/text()").get()
        if externalid:
            item_loader.add_value("external_id", externalid.split(": ")[-1])
        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.strip())
           
        address =response.xpath("//meta[@itemprop='addressLocality']/@content").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(" ")[0]
            item_loader.add_value("city", city.strip())
        zipcode=response.xpath("//meta[@itemprop='postalCode']/@content").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent=rent.replace("\n","").replace(" ","").replace("\xa0","").replace("\t","").split("€")[0].split("\\x")[0].replace(",",".").strip()
            if rent:
               item_loader.add_value("rent",int(float(rent)))

        item_loader.add_value("currency", "GBP")
        description = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        images = [x for x in response.xpath("//section[@class='annonce details']//figure//div[@id='diaporamaProfil']//div//img[@itemprop='image']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        room_count =response.xpath("//li[@class='pieces']/div/span[2]/text()").get()
        if room_count:
            room_count =re.findall("\d",room_count)
            item_loader.add_value("room_count", room_count)

        bathroom_count =response.xpath("//li[@class='sdb' or @class='salles_eau']/div/span[2]/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)

        squaremeters=response.xpath("//li[@class='surface']/div/span[2]/text()").get()
        if squaremeters:
            squaremeters=re.findall("\d+",squaremeters)
            item_loader.add_value("square_meters", squaremeters)

        deposit=response.xpath("//li[@class='margin']/following-sibling::li/text()").get()
        if deposit:
            deposit=deposit.replace("\xa0","").split(":")[1].split("€")[0].split(",")[0].strip()
            item_loader.add_value("deposit",deposit)
        else:
            deposit=response.xpath("//li[contains(.,'de garantie :')]/text()").get()
            if deposit:
                deposit=deposit.replace("\xa0","").split(":")[1].split("€")[0].split(",")[0].strip()
                item_loader.add_value("deposit",deposit)
        utilities=response.xpath("//li[contains(.,'Provision pour charge')]/text()").get()
        if utilities:
            utilities=utilities.replace("\xa0","").split(":")[1].split("€")[0].split(",")[0].strip()
            item_loader.add_value("utilities",utilities)
        item_loader.add_value("landlord_name", "Plisson immobilier")
        item_loader.add_value("landlord_phone", "+33 (0)1 45 72 99 23")


        yield item_loader.load_item()