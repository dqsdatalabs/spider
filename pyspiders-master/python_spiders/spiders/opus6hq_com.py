# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re



class MySpider(Spider):
    name = 'opus6hq_com'
    execution_type='testing'
    country='Ireland'
    locale='en'
    external_source = "Opus6hq_PySpider_Ireland"
 
    def start_requests(self):
        start_urls = [
            {   #https://opus6hq.com/apartments/   link in site redirect that url 
                "url" : [
                    "https://www.daft.ie/for-rent/opus-6-hanover-quay-hanover-quay-dublin-2-co-dublin/1442724",
                ],
                "property_type" : "apartment",
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@data-testid='sub-unit']/@href").getall():
            follow_url = "https://www.daft.ie/" + item

            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        description = "".join(response.xpath("//div[@data-testid='description']/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.replace("\r","").replace("\n","").strip())
            item_loader.add_value("description", description)



        item_loader.add_value("external_link", response.url)

        rent = response.xpath("//span[@class='TitleBlock__StyledSpan-sc-1avkvav-4 gDBFnc']/text()").get()
        if rent:
            rent = rent.split()[0].replace(",","").replace("€","").replace(" ","")
            item_loader.add_value("rent",rent)

        furnished = response.xpath("//*[text()='Furnished']").get()
        if furnished:
            item_loader.add_value("furnished",True)

        bathroom = response.xpath("//li[span[text()='Bathroom']]/text()[last()]").get()
        if bathroom:
            item_loader.add_value("bathroom_count",bathroom)

        room = response.xpath("//li[span[contains(text(),'Bedroom')]]/text()[last()]").get()
        if room:
            item_loader.add_value("room_count",room)

        item_loader.add_value("address","Hanover Quay - Dublin")
        item_loader.add_value("city","Dublin")
        title = response.xpath("//h1[@data-testid='address']/text()").get()
        if title:
            item_loader.add_value("title",title)

        position = response.xpath("//a[@aria-label='Satellite View']/@href").get()
        if position:
            lat = position.split("+")[0].split("loc:")[-1]
            long = position.split("+")[-1]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)


        images_script = response.xpath("//script[contains(text(),'size1200x1200')]").get()
        if images_script:
            images = re.findall('"size1200x1200":"([^,]+)","', images_script)
            item_loader.add_value("images",images)


        washing_machine = response.xpath("//li[contains(@class,'PropertyDetailsList')]/text()[contains(.,'Washing')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        dishwasher = response.xpath("//li[contains(@class,'PropertyDetailsList')]/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_id",response.url.split("/")[-1])
        item_loader.add_value("currency","EUR")
        item_loader.add_value("property_type","apartment")

        item_loader.add_value("landlord_phone","+353 1 618 1325")
        item_loader.add_value("landlord_name","Clarie Neary")
        item_loader.add_value("landlord_email","clarie.neary@savills.ie")



        yield item_loader.load_item()