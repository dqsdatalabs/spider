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
    name = 'reside_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.reside.co.uk/?id=6165&do=search&for=2&maxprice=99999999999&minbeds=0&type%5B%5D=8&kwa%5B%5D=&page=0&do=search&order=1&id=6165&cats=1&imageField.x=77&imageField.y=18",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://www.reside.co.uk/?id=6165&do=search&for=2&maxprice=99999999999&minbeds=0&type%5B%5D=6&kwa%5B%5D=&page=0&do=search&order=1&id=6165&cats=1&imageField.x=68&imageField.y=13",
                    "http://www.reside.co.uk/?id=6165&do=search&for=2&maxprice=99999999999&minbeds=0&type%5B%5D=15&kwa%5B%5D=&page=0&do=search&order=1&id=6165&cats=1&imageField.x=77&imageField.y=8",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "http://www.reside.co.uk/?id=6165&do=search&for=2&maxprice=99999999999&minbeds=0&type%5B%5D=14&kwa%5B%5D=&page=0&do=search&order=1&id=6165&cats=1&imageField.x=60&imageField.y=4",
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//td[@width='200']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath("./parent::tr/td[2]//img[contains(@src,'Let')]/@src").get()
            if not status or (status and "/Let." not in status):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Reside_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("=")[-1])
        

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//h1/strong/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent_week = response.xpath("//h1/strong[contains(.,'Price')]//span//text()").get()
        if rent_week and "pw" in rent_week.lower():
            rent = response.xpath("//h1/strong/text()[contains(.,'Price')]").get()
            if rent:
                rent = rent.split("£")[1].split(".")[0].strip()
                rent = int(float(rent))*4
                item_loader.add_value("rent", rent)
        else:
            rent = response.xpath("//h1/strong/text()[contains(.,'Price')]").get()
            if rent:
                rent = rent.split("£")[1].split(".")[0].strip()
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//td[@class='black'][contains(.,'bedroom')]/text()").get()
        if room_count:
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            else:
                item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//td[@class='black'][contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        deposit = response.xpath("//p//text()[contains(.,'Deposit:')]").get()
        if deposit:
            deposit = deposit.split("week")[0].strip().split(" ")[-1]
            deposit = int(deposit)*int(float(rent))
            item_loader.add_value("deposit", deposit)
        
        import dateparser
        available_date = response.xpath("//h1[@class='black']/following-sibling::div[1]/text()[contains(.,'Available')]").get()
        if available_date:
            available_date = available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        description = " ".join(response.xpath("//h1[@class='black']/following-sibling::div[2]//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='gallery_thumbnails_div']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('cbll=')[1].split(',')[0]
            longitude = latitude_longitude.split('cbll=')[1].split(',')[1].split('&')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "RESIDE")
        item_loader.add_value("landlord_phone", "020 7639 3366")
        item_loader.add_value("landlord_email", "reside@reside.co.uk")
        
        yield item_loader.load_item()