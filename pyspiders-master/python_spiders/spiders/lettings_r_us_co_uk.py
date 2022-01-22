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
    name = 'lettings_r_us_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://lettings-r-us.co.uk/plm/exec/search.cgi?search=1&sort_order=6%2C123%2Cforward&lfield31_keyword=&perpage=5&marknew=1&euro_numbers=0&lfield1_keyword=&lfield9_keyword=Flat&lfield6_min=&lfield6_max=&lfield10_min=&lfield10_max=&search.x=99&search.y=9&pagenum={}",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://lettings-r-us.co.uk/plm/exec/search.cgi?search=1&sort_order=6%2C123%2Cforward&lfield31_keyword=&perpage=5&marknew=1&euro_numbers=0&lfield1_keyword=&lfield9_keyword=House&lfield6_min=&lfield6_max=&lfield10_min=&lfield10_max=&search.x=95&search.y=21&pagenum={}",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://lettings-r-us.co.uk/plm/exec/search.cgi?search=1&sort_order=6%2C123%2Cforward&lfield31_keyword=&perpage=5&marknew=1&euro_numbers=0&lfield1_keyword=&lfield9_keyword=Studio&lfield6_min=&lfield6_max=&lfield10_min=&lfield10_max=&search.x=115&search.y=22&pagenum={}",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "http://lettings-r-us.co.uk/plm/exec/search.cgi?search=1&sort_order=6%2C123%2Cforward&lfield31_keyword=&perpage=5&marknew=1&euro_numbers=0&lfield1_keyword=&lfield9_keyword=Bedsit+and+House+Share&lfield6_min=&lfield6_max=&lfield10_min=&lfield10_max=&search.x=120&search.y=7&pagenum={}",
                ],
                "property_type" : "room",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//font[contains(.,'Property')]/../.."):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Lettings_R_Us_Co_PySpider_united_kingdom")
        status = response.xpath("//p[@class='bodytext']/img").get()
        if status and "letagree" in status:
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//td/h3/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title)
            item_loader.add_value("title", title)
        
        rent = response.xpath("//th/div[contains(.,'Price')]//text()").get()
        if rent:
            price = rent.split(":")[1].split(".")[0].strip()
            item_loader.add_value("rent_string", price)
        utilities = response.xpath("//th/div[contains(.,'Price')]//text()[contains(.,'+ ')]").get()
        if utilities:
            utilities = utilities.split("+")[1].strip().split(" ")[0].strip()
            if "£" in utilities:
                item_loader.add_value("utilities", utilities) 
        deposit = response.xpath("//th/div[contains(.,'Price')]//text()[contains(.,'deposit of ')]").get()
        if deposit:
            deposit_value = deposit.split(" deposit of ")[-1].split("week")[0].strip()
            if rent:
                price = rent.split(":")[1].split("£")[1].split(".")[0].strip()
                rent_week = int(int(price)/4)
                if deposit_value == "one":
                    item_loader.add_value("deposit", str(rent_week))
                elif deposit_value == "two":
                    item_loader.add_value("deposit", str(rent_week*2))
        external_id = response.xpath("//th/div[contains(.,'Ref')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        address = response.xpath("//b/font/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        from word2number import w2n
        room_count = response.xpath("//td/p[2]/text()").get()
        try:
            if room_count:
                if "terrace" in room_count:
                    item_loader.add_value("terrace", True)
                    
                if room_count and "bedroom" in room_count:
                    room = ""
                    if room_count and "double bedroom" in room_count.lower():
                        item_loader.add_value("room_count", 2)
                    room_count = room_count.replace("double","").split("bedroom")[0].strip().split(" ")[-1]
                    if "/" in room_count:
                        room = room_count.split("/")[0].strip()
                    else:
                        room = room_count.strip()
                    item_loader.add_value("room_count", w2n.word_to_num(room))
                elif "bed" in room_count:
                    room_count = room_count.replace("double","").split("bed")[0].strip().split(" ")[-1]
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                elif "studio" in room_count:
                    item_loader.add_value("room_count", "1")
        except:
            pass
        furnished = response.xpath("//td/b[contains(.,'Furnishing')]/parent::td/following-sibling::td/text()").get()
        if furnished:
            if "Unfurnished" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
                
        pets_allowed = response.xpath("//td/b[contains(.,'Pets')]/parent::td/following-sibling::td/text()").get()
        if pets_allowed:
            if "Yes" in pets_allowed:
                item_loader.add_value("pets_allowed", True)
            elif "No" in pets_allowed:
                item_loader.add_value("pets_allowed", False)
        
        parking = response.xpath("//td/b[contains(.,'Parking')]/parent::td/following-sibling::td[1]/text()").get()
        if parking:
            if "None" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        
        desc = "".join(response.xpath("//b[contains(.,'Main Features')]/parent::td/following-sibling::td//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
            
            if "floor" in desc:
                floor = desc.split("floor")[0].strip().split(" ")[-1]
                if "lamin" in floor:
                    floor = desc.split("floor")[1].strip().split(" ")[-1]
                if "ly" not in floor:
                    item_loader.add_value("floor", floor)
            
            from datetime import datetime
            import dateparser
            if "available now" in desc:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "available" in desc:
                available_date = desc.split("available")[1].split(".")[0].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if not date_parsed:
                    available_date = desc.split("available")[1].split("and")[0].strip()
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2) 
            
        images = [x.replace("_generic/","") for x in response.xpath("//a[contains(@href,'image')]/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "LETTINGS-R-US")
        item_loader.add_value("landlord_phone", "01373 454 188")
        
        yield item_loader.load_item()