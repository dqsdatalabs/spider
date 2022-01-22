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
    name = 'sherwoods_info'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
   
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=Flat/Apartment,style=19,ppp=6",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=End%20Terrace,style=19,ppp=6",
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=Bungalow,style=19,ppp=6",
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=Detached,style=19,ppp=6",
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=Maisonette,style=19,ppp=6",
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=Semi,style=19,ppp=6",
                    "https://www.housescape.org.uk/cgi-bin/search.pl?she1&fo=nr,type=Terraced,style=19,ppp=6"
                ],
                "property_type": "house"
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
        
        for item in response.xpath("//td[@align='center']/div//a/@href[not(contains(.,'#'))]").extract():
            url = item.split("'")[1]
            follow_url = f"https://www.housescape.org.uk/cgi-bin/{url}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Sherwoods_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("&&")[1])

        title = "".join(response.xpath("//table[contains(@class,'bannertable')]//td[contains(@align,'left')]//text()").getall())
        if title:
            title = title.strip()
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//table[contains(@class,'bannertable')]//td[contains(@align,'left')]//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        rent = response.xpath("//b[contains(.,'£')]//text()").get()
        if rent:
            rent = rent.replace("£","").replace(",","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//span[contains(.,'Description')]//parent::font//parent::td//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        rooms = response.xpath("//b[contains(.,'Bed ')]//text()").get()
        if rooms:
            room_count = rooms.split("Bed")[0].strip()
            bathroom_count = rooms.split("Bed")[1].split("Bath")[0].strip()
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        elif response.xpath("//b[contains(.,'Beds')]//text()").get():
            rooms = response.xpath("//b[contains(.,'Beds')]//text()").get()
            if rooms:
                room_count = rooms.split("Beds")[0].strip()
                bathroom_count = rooms.split("Beds")[1].split("Bath")[0].strip()
                item_loader.add_value("room_count", room_count)
                item_loader.add_value("bathroom_count", bathroom_count)
        else:
            rooms = response.xpath("//b[contains(.,'Studio')]//text()").get()
            if rooms:
                bathroom_count = rooms.split("Studio")[1].split("Bath")[0].strip()
                item_loader.add_value("room_count", "1")
                item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//a[contains(@rel,'prettyPhoto[gallery]')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//span[contains(.,'Floor plan')]//parent::font//following-sibling::table//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//b[contains(.,'£')]//parent::span//parent::font//following-sibling::font//text()").getall())
        if available_date:
            available_date = available_date.split("Available")[-1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished') or contains(.,'FURNISHED')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//li[contains(.,'floor')]//text()").get()
        if floor:
            floor = floor.split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//span[contains(.,'Energy Efficiency')]//parent::font//following-sibling::div//@src").get()
        if energy_label:
            energy_label = energy_label.split("epc1=")[1].split("&")[0]
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "Sherwoods")
        item_loader.add_value("landlord_phone", "0208 867 0031")
        item_loader.add_value("landlord_email", "sales@sherwoods.info")

        yield item_loader.load_item()