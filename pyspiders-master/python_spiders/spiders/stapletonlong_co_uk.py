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
    name = 'stapletonlong_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.stapleton-long.co.uk/search.php?propstat=rent"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 20)
        seen = False
        for item in response.xpath("//div[@class='col-lg-3']"):
            status = item.xpath("./div/img/@src").get()
            if status and ("agreed" in status.lower() or "/let" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 20 or seen:
            p_url = f"https://www.stapleton-long.co.uk/search.php?start={page}&p_f=0&beds=&minprice=&maxprice=&type=&location=&keyword=&ref=&propstat=rent"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+20})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Stapletonlong_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[contains(@class,'maincolou')]/span/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='iboxes']/text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        title = "".join(response.xpath("//div[@class='inner']//span//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
            if "Ref" in title:
                external_id = title.split("Ref no.")[1].split(")")[0].strip()
                item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//div[@class='bord']/div/center/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city","London")
         
        rent = response.xpath("//div[@class='bord']/center/span/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("£")[1].replace(",","").strip())
            item_loader.add_value("currency", "EUR")
        
        room_count = "".join(response.xpath(
            "//td/img[contains(@src,'bed')]/parent::td/following-sibling::td//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = "".join(response.xpath(
            "//td/img[contains(@src,'bath')]/parent::td/following-sibling::td//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div[@class='iboxes']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sq ft" in desc:
            square_meters = desc.split("sq ft")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", str(int(int(square_meters)* 0.09290304)))
        
        images = [x for x in response.xpath("//div[@id='slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//img/@src[contains(.,'FLP_')]").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//div[@class='iboxes']//text()[contains(.,'EPC')]").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].strip()
            item_loader.add_value("energy_label", energy_label)
        
        deposit = response.xpath(
            "//div[@class='iboxes']//text()[not(contains(.,'Holding')) and contains(.,'Deposit:')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().split(" ")[0]
            price = int(rent.split("£")[1].replace(",","").strip())/4
            item_loader.add_value("deposit", int(deposit)*price)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[@class='iboxes']//text()[contains(.,'Available')]").get()
        if available_date:
            if "immediately" in available_date or "now" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "end of" in available_date:
                available_date = available_date.split("Available end of")[-1].split(",")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d") 
                    item_loader.add_value("available_date", date2)
        
        item_loader.add_value("landlord_name", "Stapleton Long Estate Agents")
        item_loader.add_xpath("landlord_phone", "//a[@class='tel']/text()")
        item_loader.add_value("landlord_email", "norwood.sales@stapletonlong.co.uk")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None