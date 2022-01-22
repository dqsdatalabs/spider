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
    name = 'richandoliva_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    headers = {
        'authority': 'richandoliva.com.au',
        'content-length': '0',
        'accept': '*/*',
        'origin': 'https://richandoliva.com.au',
        'referer': 'https://richandoliva.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&order=dateListed-desc&paged=2&extended=1',
        'accept-language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        start_url = "https://richandoliva.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5Bpaged%5D%5Bvalue%5D=1&query%5Btype%5D%5Bvalue%5D=residential&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Bbeds%5D%5Bvalue%5D=&query%5Bbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5BunderAgency%5D%5Bvalue%5D="
        yield FormRequest(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["data"]["listings"]:
            seen = True
            follow_url = item["url"]
            property_type = item["post_content"]
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type), "item":item})

        if page == 2 or seen: 
            f_url = f"https://richandoliva.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Btype%5D%5Bvalue%5D=residential&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Bbeds%5D%5Bvalue%5D=&query%5Bbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5BunderAgency%5D%5Bvalue%5D="
            yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Richandoliva_Com_PySpider_australia")       
        item = response.meta.get('item')
        item_loader.add_value("title", item["title"])
        item_loader.add_value("latitude", item["lat"])
        item_loader.add_value("longitude", item["long"])
        item_loader.add_value("room_count", item["detailsBeds"])
        item_loader.add_value("bathroom_count", item["detailsBaths"])
        address = item["displayAddress"]
        item_loader.add_value("address", address)
        item_loader.add_value("city", address.split(",")[-1].strip())
        rent = item["price"]
        if "DEPOSIT TAKEN" in rent.upper():
            return
        if rent:            
            rent = rent.split("$")[-1].replace("#","").lower().split('p')[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'USD')
        if "floor" in item["post_content"]:
            floor = item["post_content"].split("floor")[0].strip().split(" ")[-1]
            if "tiled" not in floor and "timber" not in floor:
                item_loader.add_value("floor", floor)
       
        deposit = response.xpath("//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'bond')]/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip())
        external_id = response.xpath("//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
    
        parking = response.xpath("//p[contains(@class,'car')]/text()").get()
        if parking:
            if parking.strip() != "0": item_loader.add_value("parking", True)
            else: item_loader.add_value("parking", False)
        
        available_date = response.xpath("//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("available")[1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='b-description__text']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        images = [x for x in response.xpath("//div[@id='media-gallery']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        landlord_name = response.xpath("//h5[contains(@class,'card-title')]/a/text()").get()
        landlord_phone = response.xpath("//span[contains(@class,'phone-number')]/a/text()").get()
        landlord_email = response.xpath("//p/a[contains(@href,'mail')]/text()[contains(.,'@')]").get()
          
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        if not item_loader.get_collected_values("available_date"):
            available_date = response.xpath("//strong[contains(.,'available')]/following-sibling::text()").get()
            if available_date:
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None