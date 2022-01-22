# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, scrapy
import re
import dateparser
from python_spiders.helper import ItemClear
class MySpider(Spider):
    name = 're88_com_au'
    execution_type = 'testing'
    country = 'australia'
    locale = 'en'
    external_source = 'Re88_Com_PySpider_australia'

    def start_requests(self):
        start_url = "https://re88.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bextended%5D%5Bvalue%5D=0"
        yield FormRequest(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["data"]["listings"]:
            seen = True
            follow_url = item["url"]
            
            yield Request(follow_url, callback=self.populate_item, meta={"item":item})
        
        if page == 2 or seen:
            p_url = f"https://re88.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Bextended%5D%5Bvalue%5D=0"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item = response.meta.get("item")

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        prop = response.xpath("//h5[@class='single-post-title']/text()").get()
        if prop and ("apartment" in prop.lower() or "flat" in prop.lower()):
            property_type = "apartment"
        elif prop and ("house" in prop.lower()):
            property_type = "house"
        elif prop and ("studio" in prop.lower()):
            property_type = "studio"
        else:
            property_type = "apartment"
        item_loader.add_value("property_type", property_type)
       
        
        title = item["title"]
        item_loader.add_value("title", title)
        
        external_id = response.xpath("(//strong[contains(.,'property ID')]/following-sibling::text())[1]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        rent = response.xpath("(//p[@class='listing-info-price']/text())[1]").get()
        if rent and "week" in rent.lower():
            rent = rent.split(" ")[0].replace("$", "")
            item_loader.add_value("rent", int(rent)*4)
        else:
            rent = item["price"].replace("$", "")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "USD")
        
        room_count = item["detailsBeds"]
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = item["detailsBaths"]
        item_loader.add_value("bathroom_count", bathroom_count)
        
        address = item["mapAddress"]
        item_loader.add_value("address", address)
        
        latitude = item["lat"]
        item_loader.add_value("latitude", latitude)
        
        longitude = item["long"]
        item_loader.add_value("longitude", longitude)

        parking = item["detailsCarAccom"]
        if parking:
            if int(parking) > 0:
                item_loader.add_value("parking", True)

        desc = "".join(response.xpath("//div[@class='section-body post-content']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc)

        deposit = response.xpath("(//strong[contains(.,'bond price')]/following-sibling::text())[1]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("$", "").strip())
        
        available_date = response.xpath("(//strong[contains(.,'date available')]/following-sibling::text())[1]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        images = [x.split("('")[-1].split("')")[0] for x in response.xpath("//div[contains(@class,'single-listing-slick-slide')]/@style").extract()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("//h5[@class='staff-card-title']/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_phone = response.xpath("//span[contains(@class,'phone-number')]/@data-phonenumber").get()
        if landlord_name:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        item_loader.add_value("landlord_email", "leasing@re88.com.au")

        yield item_loader.load_item()