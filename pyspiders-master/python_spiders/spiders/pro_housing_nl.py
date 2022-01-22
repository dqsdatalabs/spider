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
    name = 'pro_housing_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "ProHousing_PySpider_netherlands_nl"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.pro-housing.nl/nl/te-huur",
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='houseCardText']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type = response.xpath("//td[contains(.,'Soort')]//following-sibling::td//text()").get()
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        elif property_type and "woonhuis" in property_type.lower():
            item_loader.add_value("property_type","house")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id = "".join(response.url)
        if external_id:
            external_id = external_id.split("/")[-1:]
            item_loader.add_value("external_id",external_id)

        address = response.xpath("//section[@id='rentcontent']//div[@class='container']//h1/text()").get()
        if address:
            item_loader.add_value("address",address)

        city = response.xpath("//section[@id='rentcontent']//div[@class='container']//h1/text()").get()
        if city:
            if city and "," in city:
                city = city.split(",")[1]
            item_loader.add_value("city",city)

        square_meters = response.xpath("//td[contains(.,'Woonoppervlakte')]//following-sibling::td//text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m²")[0]
            item_loader.add_value("square_meters",square_meters)

        description = response.xpath("//div[contains(@class,'DesciptionPrice row')]//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        room_count = response.xpath("//td[contains(.,'Aantal slaapkamers')]//following-sibling::td//text()").get()
        if room_count:
            room_count = room_count.split(":")[1].split("slaapkamer(s)")[0]
            item_loader.add_value("room_count",room_count)
            
        rent=response.xpath("//td[contains(.,'Huurprijs')]//following-sibling::td//text()").get()
        if rent:
            rent = rent.strip().split(":")[1].split("€")[1].split(",")[0]
            rent = rent.replace(".","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        
        deposit=response.xpath("//td[contains(.,'Borgsom')]//following-sibling::td//text()").get()
        if deposit:
            deposit = deposit.strip().split(":")[1].split("€")[1].split(",")[0]
            deposit = deposit.replace(".","")
            item_loader.add_value("deposit",deposit)

        available_date = response.xpath("//td[contains(.,'Aanvaardingsdatum')]//following-sibling::td//text()").get()
        if available_date:
            available_date = available_date.strip().split(":")[1]
            item_loader.add_value("available_date",available_date) 

        elevator = response.xpath("//td[contains(.,'Lift')]//following-sibling::td//text()").get()
        if elevator and "ja" in elevator.lower():
            item_loader.add_value("elevator",True)
        else:
            item_loader.add_value("elevator",False)

        furnished = response.xpath("//td[contains(.,'Inrichting')]//following-sibling::td//text()").get()
        if furnished and "gestoffeerd" in furnished.lower():
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)

        images = [response.urljoin(x) for x in response.xpath("//a[@class='swipebox']//@href").getall()]
        if images:
            item_loader.add_value("images", images)    

        item_loader.add_value("landlord_name","Pro Housing")
        item_loader.add_value("landlord_phone","+31 (0)43 362 76 74")
        item_loader.add_value("landlord_email","info@pro-housing.nl")

        yield item_loader.load_item()