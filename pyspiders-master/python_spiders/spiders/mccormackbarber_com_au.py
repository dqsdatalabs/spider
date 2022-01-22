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
class MySpider(Spider):
    name = 'mccormackbarber_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'     
    external_source = "Mccormackbarber_Com_PySpider_australia"  
    headers = {
        'authority': 'mccormackbarber.com.au',
        'accept': '*/*',
        'x-requested-with': 'XMLHttpRequest',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://mccormackbarber.com.au/residential/lease',
        'accept-language': 'tr,en;q=0.9',
        'Cookie': '_WHEELS_AUTHENTICITY=ULaO7W4MM5kryr%2BzTPlA1033NoyMEXEBPfYz3NMJ0Rxed8AJMtwlfqn07oehprwBoJX%2FavytdL1NbIYxs2CR4SKI0S%2FMLVsKc2wtix%2Fr6Ma94GQlH573UQ18K89qICfVluh4XmPzOTxTPsAv6cYLcA%3D%3D; FLASH=%7B%7D'
    }

    def start_requests(self):
        start_url = "https://mccormackbarber.com.au/data/results/?listing_category=Residential&listing_sale_method=Lease&deletecache=0&pg=1"
        yield Request(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        selector = Selector(text=response.body, type="html")
        for item in selector.xpath("//a[contains(@class,'list')]/@href").getall():
            follow_url = "https://mccormackbarber.com.au" + item.replace("\\", "").replace("\"", "")
            seen = True
            yield Request(follow_url, callback=self.populate_item)

        if page == 2 or seen: 
            follow_url = response.url.replace("&pg=" + str(page - 1), "&pg=" + str(page))
            yield FormRequest(follow_url, headers=self.headers, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "".join(response.xpath("//div[@class='text']//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("real-estate/")[1].split("/")[0])
        item_loader.add_value("external_source", self.external_source)  
        title = response.xpath("//div[@class='left']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace("\n",""))        
        item_loader.add_xpath("room_count", "//span[@class='bed']/h1/text()")
        item_loader.add_xpath("bathroom_count", "//span[@class='bath']/h1/text()")
        item_loader.add_xpath("deposit", "//tr[td[.='Bond:']]/td[2]/span/text()") 
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            if "per week" in rent.lower():
                rent = rent.split("$")[1].lower().split("p")[0].strip()
                item_loader.add_value("rent", int(float(rent)) * 4)
            else:
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'USD')
 
        address = response.xpath("//div[@class='left']/h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip().replace("\n",""))
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city.strip()) 
        parking = response.xpath("//span[@class='car']/h1/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        
        available_date = response.xpath("//tr[@class='available']/td[2]/span/text()").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='text']/span//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//section[@id='listing-show-oscar']/img[@id='image-for-print']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'lat:') and contains(.,'lng:')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", script_map.split("lng:")[1].split("}")[0].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='agents']//ul/div[1]//h1/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agents']//ul/div[1]//h2[last()-1]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='agents']//ul/div[1]//h2[contains(.,'@')]/text()")

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