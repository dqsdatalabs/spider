# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'soames_com'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://soames.com/properties-for-lease?class=apartment&searchtype=lease&orderby=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://soames.com/properties-for-lease?class=house&searchtype=lease&orderby=",
                    "http://soames.com/properties-for-lease?class=townhouse&searchtype=lease&orderby=",
                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@ga-event-key='property']/a"):
            status = item.xpath(".//img[@class='slash']/@alt").get()
            if status and "leased" in status.lower():
                continue
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'sd next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Soames_PySpider_australia")
        item_loader.add_value("external_id", response.url.split(".com/")[1].split("/")[0])
    
        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@itemprop,'streetAddress')]//text()").get()
        item_loader.add_value("address", address)
        city = response.xpath("//span[contains(@itemprop,'addressLocality')]//text()").get()
        item_loader.add_value("city", city)

        rent = response.xpath("//span[contains(@class,'muted')]//text()").get()
        if rent and "$" in rent:
            rent = rent.replace(",","").split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "USD")

        deposit = "".join(response.xpath("//b[contains(.,'Bond:')]/parent::div//text()").getall())
        if deposit:
            deposit = deposit.split("$")[1].strip()
            item_loader.add_value("deposit", deposit)


        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//b[contains(.,'Availability:')]/parent::div//text()").getall())
        if available_date:
            available_date = available_date.split("Availability:")[1].strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        room_count = response.xpath("//section//div[@class='icons hide-med']//i[contains(@class,'icon-bed')]/parent::div/text()[2]").get()        
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//section//div[@class='icons hide-med']//i[contains(@class,'icon-bath')]/parent::div/text()[3]").get()        
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//section//div[@class='icons hide-med']//i[contains(@class,'icon-car')]/parent::div/text()[4]").get()        
        if parking:
            item_loader.add_value("parking", True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'7')]/div[@class='contentRegion']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor " in desc:
                floor = desc.split("floor ")[0].strip().split(" ")[-1]
                not_list = ["polished","new","open"]
                status = True
                for i in not_list:
                    if i in floor.lower():
                        status = False
                if status:
                    print(floor)
                    item_loader.add_value("floor", floor)
        
        balcony = " ".join(response.xpath("//li[contains(.,'Balcony')]//text()").getall())
        if balcony:
            balcony = re.sub('\s{2,}', ' ', balcony.strip())
            item_loader.add_value("balcony", True)

        swimming_pool = " ".join(response.xpath("//li[contains(.,'Pool')]//text()").getall())
        if swimming_pool:
            swimming_pool = re.sub('\s{2,}', ' ', swimming_pool.strip())
            item_loader.add_value("swimming_pool", True)

        images = [x for x in response.xpath("//div[contains(@id,'slideshow')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude = response.xpath("//meta[contains(@property,'latitude')]/@content").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = response.xpath("//meta[contains(@property,'longitude')]/@content").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        landlord_name = response.xpath("//div[contains(@class,'agent')]//span[contains(@itemprop,'name')]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@class,'agent')]//a[contains(@itemprop,'email')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent')]//a[contains(@itemprop,'telephone')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        # yield item_loader.load_item()