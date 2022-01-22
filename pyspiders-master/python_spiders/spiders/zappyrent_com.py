# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.http.headers import Headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'zappyrent_com'
    external_source = "Zappyrent_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    start_url=["https://www.zappyrent.com/it"]
    # custom_settings = {
    #     "PROXY_TR_ON" : True
    # }
    def start_requests(self):
        yield Request(
            url=self.start_url[0],
            callback=self.jump,
       
        )

    # 1. FOLLOWING
    def jump(self, response):
        for item in response.xpath("//section[@id='our-cities']//li"):
            follow_url = response.urljoin(item.xpath(".//a//@href").get())

            yield Request(
                follow_url, 
                callback=self.parse, 
            )
        
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'h-full cursor-pointer')]"):
            follow_url = response.urljoin(item.xpath(".//a//@href").get())

            property_type = item.xpath(".//div//p//text()").get()
            if "trilocale" in property_type.lower():
                property_type = "apartment"
            elif "bilocale" in property_type.lower():
                property_type = "house"
            elif "quadrilocale" in property_type.lower():
                property_type = "apartment"
            elif "monolocale" in property_type.lower():
                property_type = "studio"
            elif "stanza" in property_type.lower():
                property_type = "room"
            else:
                property_type = "apartment"
            
            if property_type != "":

                yield Request(
                    follow_url, 
                    callback=self.populate_item,
                    meta={"property_type" : property_type} 
                )

        next_page = response.xpath("//li[contains(@class,'next')]//a//@href").get()
        p_url = response.urljoin(next_page)
        if next_page:
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True
            )

            
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)


        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")

        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)

        description = response.xpath(
            "//div[contains(@class,'flex flex-col w-full lg:w-11/12 lg:px-2')]//following-sibling::span//text()").getall()
        if description:
            item_loader.add_value("description", description)

        address = response.xpath("//h1[contains(@class,'h3 pr-2 border-r border-gray-400')]//text()").get()
        if address:
            item_loader.add_value("address", address.split('Via')[-1].split('via')[-1].strip())

        city = response.xpath(
            "//h1[contains(@class,'h3 pr-2 border-r border-gray-400')]//text()").get()
        if city:
            city=city.split(",")[1]
            item_loader.add_value("city", city.strip())

        rent = response.xpath(
            "//p[contains(@class,'pl-2')]//text()").get()
        if rent:
            rent=rent.split("€")[0]
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

        utilities = response.xpath("(//span[contains(.,'Spese')]/following-sibling::span/text())[1]").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        if prop_type == "studio":
            room_count = 1
        elif prop_type == "room":
            room_count = 1
        else:
            room_count = response.xpath("(//div[@class='flex flex-row items-center pr-2'][contains(.,'Stanz')]/text())[1]").get()
        
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("(//div[@class='flex flex-row items-center pr-2'][contains(.,'Bagn')]/text())[1]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        available_date = response.xpath("//div[@class='pt-1 text-gray-600 w-full text-xs']/text()").get()
        if available_date:
            available_date = available_date.split('disponibile dal ')[1].split(' ')[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        details = "".join(response.xpath("(//span[@class='text-base font-light text-gray-600 pb-3']/text())[1]").getall())
        if details:
            if details and "ascensore" in details.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
            if details and "balcone" in details.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
            if details and "parcheggio" in details.lower():
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        washing_machine = response.xpath("//span/text()[contains(.,'Lavatrice')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        else:
            item_loader.add_value("washing_machine", False)

        dishwasher = response.xpath("//span/text()[contains(.,'Lavastoviglie')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        else:
            item_loader.add_value("dishwasher", False)

        json_data = response.xpath("//script[@type='application/json']/text()[contains(.,'uploads')]").get()
        if json_data:
            data = json.loads(json_data)["props"]["pageProps"]     
            images = [x["url"] for x in data["property"]["images"]]
            if images:
                item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(@type,'application/ld+json')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('"longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "+39 02 9475 5562")
        item_loader.add_value("landlord_email", "assistenza@zappyrent.com")
        item_loader.add_value("landlord_name", "ZAPPYRENT")

        yield item_loader.load_item()

