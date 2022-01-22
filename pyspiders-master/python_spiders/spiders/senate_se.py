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
    name = 'senate_se'
    external_source = "Senate_PySpider_sweden"
    execution_type = 'testing'
    country = 'sweden' 
    locale ='sv' 
    start_urls = ['https://senate.se/lediga-bostader/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='grid']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath(".//div[@class='status-ledig']/p/text()").get()
            if "ledig" in status.lower():
                yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("external_source", self.external_source)


        # external_id =response.xpath("//div[contains(@class,'snabbfakta')]//p[contains(.,'Objekt nr:')]//following-sibling::text()").get()
        # if external_id:
        #     item_loader.add_value("external_id",external_id)

        rent =response.xpath("//div[contains(@class,'snabbfakta')]//p[contains(.,'Hyra:')]//following-sibling::text()").get()
        if rent:
            rent=rent.split(" ")[0]
            item_loader.add_value("rent",rent)
            item_loader.add_value("currency","SEK")

        title = "".join(response.xpath("//title//text()").get())
        if title:
            title=title.replace("\u00e5","").replace("\u00e4","").replace("\u00f6","")
            item_loader.add_value("title",title)

        address = "".join(response.xpath("//div[contains(@class,'col-md-8')]//h1//text()").get())
        if address:
            title=title.replace("\u00e5","").replace("\u00e4","").replace("\u00f6","")
            item_loader.add_value("address",address)
            city=address.split(" ")[0]
            item_loader.add_value("city",city)
            zipcode=address.split(" ")[1]
            item_loader.add_value("zipcode",zipcode)

        room_count = response.xpath("//div[contains(@class,'snabbfakta')]//p[contains(.,'Antal rum:')]//following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        square_meters = response.xpath("//div[contains(@class,'snabbfakta')]//p[contains(.,'Storlek:')]//following-sibling::text()").get()
        if square_meters:
            square_meters=square_meters.split("m")[0]
            item_loader.add_value("square_meters",square_meters)

        description = "".join(response.xpath("//div[contains(@class,'col-md-8')]//p//text()").getall())
        if description:
            description=description.replace("\u00e5","").replace("\u00e4","").replace("\u00f6","")
            item_loader.add_value("description",description)

        balcony = "".join(response.xpath("//div[contains(@class,'snabbfakta')]//p[contains(.,'Balkong:')]//following-sibling::text()").get())
        if 'Nej' in balcony:
            item_loader.add_value("balcony",False)
        else:
            item_loader.add_value("balcony",True)
            
        parking = "".join(response.xpath("//div[contains(@class,'snabbfakta')]//p[contains(.,'Parkering:')]//following-sibling::text()").get())
        if 'Nej' in parking:
            item_loader.add_value("parking",False)
        else:
            item_loader.add_value("parking",True)

        images = [response.urljoin(x)for x in response.xpath("//ul[contains(@class,'slider')]//li//img[contains(@class,'attachment-full size-full')]//@src").getall()]
        if images:
                item_loader.add_value("images",images)

        item_loader.add_value("landlord_phone", "0510-488 711")
        item_loader.add_value("landlord_email", "lidkoping@senate.se")
        item_loader.add_value("landlord_name", "Senate")

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value("external_id",external_id)

        available_date = response.xpath("//b[contains(text(),'Tillträde:')]/following-sibling::text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date.strip())

        yield item_loader.load_item()