# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy

class MySpider(Spider):
    name = 'beesnees_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    url = "https://www.beesnees.com.au/wp-admin/admin-ajax.php"
    headers = {
        'authority': 'www.beesnees.com.au',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'x-requested-with': 'XMLHttpRequest',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.beesnees.com.au',
        'referer': 'https://www.beesnees.com.au/tenants/property-search/',
        'accept-language': 'tr,en;q=0.9'
    }
    formdata = {
        "action": "propertyPosts",
        "page": "1",
        "posts_per_page": "30",
        "property_type": "rental",
        "property_status": "current"
    }

    def start_requests(self):
        yield FormRequest(self.url, headers=self.headers, formdata=self.formdata, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        count = response.meta.get("count", 0)
        data = json.loads(response.body)
        max_page = data["max_pages"]

        selector = scrapy.Selector(text=data["html"], type="html")
        for item in selector.xpath("//div[@data-mh='property']"):
            follow_url = item.xpath("./a/@href").get()
            property_type = item.xpath(".//span[@class='underContract']/span/text()").get()
            if follow_url:
                if property_type:
                    if get_p_type_string(property_type):
                        count = count+1
                        yield Request(follow_url, dont_filter=True, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type), "count": count})
            else:
                if property_type:
                    count = count+1
                    alt_url = "https://www.beesnees.com.au/tenants/property-search/"
                    if get_p_type_string(property_type): yield Request(alt_url, dont_filter=True, callback=self.populate_item2, meta={"property_type":get_p_type_string(property_type), "count": count})

        if page <= max_page:
            self.formdata["page"] = str(page)
            yield FormRequest(self.url, dont_filter=True, headers=self.headers, formdata=self.formdata, callback=self.parse, meta={"page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "tenants/property-search/" in response.url:
            return

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Beesnees_Com_PySpider_australia")

        address = response.xpath("//div[@class='title']//text()").get()
        item_loader.add_value("address", address)

        item_loader.add_xpath("city", "//div[@class='subTitle']/text()")
        
        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        desc = "".join(response.xpath("//div[contains(@class, 'group')]/p//text()").getall())
        item_loader.add_value("description", desc)

        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)

        external_id = response.xpath("//div[contains(@class, 'group')]/p//text()[contains(.,'Property Code')]").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        rent = response.xpath("//p[contains(@class,'item price')]//text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[1].split(" ")[0]
            if "/" in rent: rent = rent.split("/")[0].strip()
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")

        room_count = "".join(response.xpath("//li[contains(@class,'item bedrooms')]//text()").getall())
        if room_count:
            room_count = room_count.split(":")[1].strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//li[contains(@class,'item bathrooms')]//text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = "".join(response.xpath("//li[contains(@class,'item carspaces')]//text()").getall())
        if parking:
            parking = parking.split(":")[1].strip()
            if parking !='0':
                item_loader.add_value("parking", True)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p[contains(@class,'item price')]//text()").getall())
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slider')]//li//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//p/strong[contains(.,'Furnished') or contains(.,' furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        landlord_name = response.xpath("//div[contains(@class,'agentDetails')]//h3[contains(@class,'title')]//text()").get()
        item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[contains(@class,'agentDetails')]//p//@href[contains(.,'tel')]").get()
        item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//div[contains(@class,'agentDetails')]//p//@href[contains(.,'mailto')]").get()
        item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

    def populate_item2(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Beesnees_Com_PySpider_australia")

        if "tenants/property-search/" in response.url:
            return

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        count = response.meta.get("count")
        
        title = response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//div[@class='title']/text()").get()
        item_loader.add_value("title", title)
        address = response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//div[@class='subTitle']/text()").get()
        item_loader.add_value("address", address)
        rent = response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//p[@class='price']/text()").get()
        if rent:
            rent = rent.split("$")[1].split(" ")[0].replace("/week","")
            item_loader.add_value("rent", int(rent)*4)
            item_loader.add_value("currency", "USD")
        
        room_count = "".join(response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//li[contains(.,'Bedroom')]//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
            
        bathroom_count = "".join(response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//li[contains(.,'Bathroom')]//text()").getall())
        if room_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        
        description = response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//p[@class='description']/text()").get()
        item_loader.add_value("description", description)
        
        import dateparser
        available_date = response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//p[@class='availability']/text()").get()
        if available_date:
            available_date = available_date.strip().split(" ")[-1]
            if "now" not in available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        parking = "".join(response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//li[contains(.,'Bathroom')]//text()").getall())
        if parking:
            parking = parking.split(":")[1].strip()
            if parking !='0':
                item_loader.add_value("parking", True)
            
        images = response.xpath(f"//div[contains(@class,'property-uniqueID-0')][{count}]//div[@class='image']/@style").get()
        if images:
            images = images.split("url(")[1].split(")")[0]
            item_loader.add_value("images", images)

        if title:
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None