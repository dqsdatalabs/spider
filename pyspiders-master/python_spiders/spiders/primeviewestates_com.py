# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'primeviewestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Primeviewestates_PySpider_united_kingdom'
    start_urls = ['https://www.primeviewestates.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&page=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for url in response.xpath("//div/a[contains(.,'Full Details')]/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            url = f"https://www.primeviewestates.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//section[@class='fullDetailWrapper']/article//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        item_loader.add_value("external_source", "Primeviewestates_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//h3//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h3//text()").get()
        if address:
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = "".join(response.xpath("//div[contains(@class,'Price')]//div[1]/text()").getall())
        if rent:
            rent = rent.strip().split("£")[1].replace(",","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//div[contains(@class,'Price')]//div//text()[contains(.,'Security Deposit')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().split(" ")[0]
            rent_week = int(rent)/4
            deposit = rent_week * int(deposit)
            item_loader.add_value("deposit", int(float(deposit)))

        desc = " ".join(response.xpath("//h2[contains(.,'Summary')]//parent::article/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'Rooms')]//span[contains(.,'bed')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'Rooms')]//span[contains(.,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'gallery')]//div[contains(@id,'property-photos-device')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'FURN')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//li[contains(.,'floor') or contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        item_loader.add_value("landlord_name", "Primeview")
        item_loader.add_value("landlord_phone", "0208 923 8884")
        item_loader.add_value("landlord_email", "lettings@primeview.co.uk")
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None