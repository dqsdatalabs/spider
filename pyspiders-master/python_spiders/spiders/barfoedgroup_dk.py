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
import dateparser

class MySpider(Spider):
    name = 'barfoedgroup_dk'
    execution_type = 'testing'
    country = 'denmark'
    locale ='da'
    start_urls = ['https://barfoedgroup.dk/ledige-lejeboliger/'] # LEVEL 1
    custom_settings = {
        "HTTPCACHE_ENABLED": False
    }
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        border=response.xpath("//a[@class='page-numbers'][last()]/text()").get()
        seen = False

        detail_xpath = response.xpath("//a[@class='apartment-box']")
        
    
      
        
        for item in detail_xpath:          
                follow_url = response.urljoin(item.xpath("./@href").get())
                yield Request(follow_url, callback=self.populate_item)
                seen = True
        if page == 2 or seen:
            if page<=int(border):
                detail_xpath = response.xpath("//section[@class='apartment-overview']")
                url = f"https://barfoedgroup.dk/ledige-lejeboliger/?areas=&rent=&size=&rooms=rooms1&other=&page_number={page}"
            
                # url = "https://barfoedgroup.dk/wp-admin/admin-ajax.php"
                # formdata = {
                #     "action": "filter_apartments",
                #     "form_values": "_wpnonce=e3e855d11c&_wp_http_referer=%2Fledige-lejeboliger%2F&search_type=private&rent=&size=&number-rooms=rooms1&takeover_date=",
                #     "page_number": f"{page}",
                #     "hasNotFiltered": "true",

                # }
                yield Request(
                    url,
                    # formdata=formdata,
                    callback=self.parse,
                    meta={"page": page+1,"detail-xpath":detail_xpath}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_desc =  " ".join(response.xpath("//div[@class='apartment-cover__description']//text()").getall())
        if property_desc and get_p_type_string(property_desc):
            item_loader.add_value("property_type", get_p_type_string(property_desc))
        else:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Barfoedgroup_PySpider_denmark")
        item_loader.add_xpath("title", "//div[@class='apartment-cover__title']/div[1]/text()")
        address = "".join(response.xpath("//div[@class='apartment-cover__title']/div[1]//text()").getall())
        if address:
            city = address.split(",")[-1].strip().split(" ")[0]
            zipcode = " ".join(address.split(",")[-1].strip().split(" ")[1:])
            item_loader.add_value("address",address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        room_square = " ".join(response.xpath("//div[@class='apartment-cover__details__wrap'][contains(.,'Størrelse')]//div[@class='apartment-cover__details__price']/text()").getall())
        if room_square and room_square.strip():
            room = room_square.split("værelser")[0]
            sqm = room_square.split(" - ")[-1].split("m")[0]
            item_loader.add_value("room_count", room)
            item_loader.add_value("square_meters", sqm)
        else:
            room_count = response.xpath("//h1[contains(.,'værelses')]/text()").get()
            if room_count:
                room_count = room_count.split("værelses")[0].strip().split(" ")[-1]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)

        price = response.xpath("//div[@class='apartment-cover__details__wrap'][contains(.,'Månedlig leje')]//div[@class='apartment-cover__details__price']/text()").get()
        if price:
            rent = price.replace(".","").split(",")[0]
            item_loader.add_value("rent", rent.strip())
        item_loader.add_value("currency", "DKK")
        deposit = response.xpath("//div[@class='apartment-cover__details__wrap'][contains(.,'Depositum')]//div[@class='apartment-cover__details__price']/text()").get()
        if deposit:
            deposit = deposit.replace(".","").split(",")[0]
            item_loader.add_value("deposit", deposit.strip())


        available_date= response.xpath("//div[@class='apartment-cover__title--small']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1].replace(".","").strip(), date_formats=["%m-%d-%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        description = " ".join(response.xpath("//div[@class='apartment-cover__description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        parking =  response.xpath("//li/span[contains(.,'Parkering')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)   
        
        elevator =  response.xpath("//li/span[contains(.,'Elevator')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)   

        washing_machine =  response.xpath("//li/span[contains(.,'Vaskeri')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)   

        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'=')").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        item_loader.add_xpath("energy_label", "substring-after(//li/span[contains(.,'Energimærkning:')]/text(),': ')")

        images = [x for x in response.xpath("//div[@class='apartment-cover__sidebar-image-container']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_xpath("landlord_name", "//div[@class='apartment-cover__sidebar']//p[a[contains(@href,'tel')]]/text()[3]")       
        item_loader.add_xpath("landlord_phone", "//div[@class='apartment-cover__sidebar']//p[a[contains(@href,'tel')]]/a[1]/text()")       
        item_loader.add_xpath("landlord_email", "//div[@class='apartment-cover__sidebar']//p/a[contains(@href,'mail')]/text()")       
    
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
   
    if p_type_string and ("lejlighed" in p_type_string.lower() or "lejlighed" in p_type_string.lower()):
        return "apartment"
    if p_type_string and ("rækkehus" in p_type_string.lower() or "tvillingehus" in p_type_string.lower() or "parcelhus" in p_type_string.lower() or "dobbelthus" in p_type_string.lower() or "pyramidehus" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
