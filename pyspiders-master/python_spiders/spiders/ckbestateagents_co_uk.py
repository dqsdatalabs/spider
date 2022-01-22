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
    name = 'ckbestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.ckbestateagents.co.uk/results']  # LEVEL 1

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "ASP.NET_SessionId=wngw5xn30zuggfge30jzxbz3; _ga=GA1.3.195692825.1616138983; _gid=GA1.3.180905754.1616138983; _gat_gtag_UA_119683019_21=1",
        "Origin": "https://www.ckbestateagents.co.uk",
        "Referer": "https://www.ckbestateagents.co.uk/results",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    }
    
    form_data = {
        "propsearchtype": "",
        "searchurl": "/results",
        "market": "1",
        "ccode": "UK",
        "view": "",
        "pricetype": "3",
        "pricelow": "",
        "pricehigh": "",
        "propbedr": "",
        "propbedt": "",
        "proptype": "",
        "area": "",
        "statustype": "1",
    }
    def start_requests(self):
        start_urls = [
            {
                "type": [
                    "Flat",
                    "Apartment",
                ],
                "property_type": "apartment"
            },
	        {
                "type": [
                    "House",
                    "Bungalow",
                    "Conversion",
                    "Detached",
                    "Duplex",
                    "End Of Terrace",
                    "House Share",
                    "Retirement",
                    "Maisonette",
                    "Semi Detached",
                    "Terraced",
                ],
                "property_type": "house"
            },
            {
                "type": [
                    "Studio",
                ],
                "property_type": "studio"
            },
            {
                "type": [
                    "Room To Let",
                ],
                "property_type": "room"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.form_data["proptype"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    callback=self.parse,
                    formdata=self.form_data,
                    headers=self.headers,
                    meta={'property_type': url.get('property_type')}
                )
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='results-list-item']//@href").extract():
            follow_url = response.urljoin(item)
            if "tenant-fees" not in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Ckbestateagents_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("ckbe-")[1].split("/")[0])

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h1/a/text()").getall())
        if address:
            address = address.replace("-","").strip()
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-2].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//span[contains(@class,'price')]/text()").get()
        if rent:
            if " pa" in rent:
                return
            rent = rent.replace("£","").split(" ")[0].replace(",","").split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bedroom')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bathroom')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides-container')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@id,'detail-floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        latitude_longitude = response.xpath("//script[contains(.,'latitude')]//text()[not(contains(.,'CKB Estates'))]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":"')[1].split('"')[0]
            longitude = latitude_longitude.split('longitude":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "CKB ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "020 3701 2822")
        item_loader.add_value("landlord_email", "Sydenham@ckbestateagents.co.uk")

        yield item_loader.load_item()