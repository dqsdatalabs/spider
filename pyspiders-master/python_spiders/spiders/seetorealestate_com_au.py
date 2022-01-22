# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'seetorealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://seetore.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bpaged%5D%5Btype%5D=equal&query%5Bextended%5D%5Bvalue%5D=1&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5Bminbeds%5D%5Bvalue%5D=&query%5Bmaxbeds%5D%5Bvalue%5D=&query%5Bbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bsubcategory%5D%5Bvalue%5D=&query%5Blandsize%5D%5Bvalue%5D=",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        data = json.loads(response.body)
        for item in data["data"]["listings"]:
            yield Request(item["url"], callback=self.populate_item, meta={"property_type":response.meta["property_type"], "item":item})
            seen = True
        
        if page == 2 or seen:
            f_url = response.url.replace(f"paged%5D%5Bvalue%5D={page-1}", f"paged%5D%5Bvalue%5D={page}")
            yield Request(f_url, callback=self.parse, meta={"property_type": response.meta.get('property_type'), "page":page+1})
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        rented = response.xpath("//div[@class='property-status']/span/text()[contains(.,'leased')]").get()
        if rented:
            return
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Seetorealestate_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")        
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//strong[contains(.,'code')]/following-sibling::span/text()", input_type="F_XPATH")        
        address = response.xpath("//p[contains(@class,'address')]/text()").get()
        if address:
            city = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        
        desc = " ".join(response.xpath("//div[contains(@class,'post-content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "studio" in desc.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1].replace("th","").replace("rd","").replace("st","").replace("nd","")
            if floor.replace("-","").replace("(","").isdigit():
                item_loader.add_value("floor", floor.replace("-","").replace("(",""))
        
        count = desc.count("sqm")
        if count == 1:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1].replace("(","").replace("+","")
            item_loader.add_value("square_meters", square_meters)
        elif count >1:
            square_meters = desc.split("sqm")[1].strip().split(" ")[-1].replace("+","").replace("(","")
            item_loader.add_value("square_meters", square_meters)
            
        rent = response.xpath("//p[contains(@class,'listing-info-price')]/text()").get()
        if "pw" in rent.lower():
            rent = rent.split(" ")[0].replace("$","").lower().replace("pw","")
            item_loader.add_value("rent", int(float(rent))*4)
        
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[contains(@class,'icon-bed')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(@class,'icon-bath')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'bond')]/following-sibling::text()", input_type="F_XPATH", replace_list={"$":""})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[contains(@class,'icon-car')]//text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat:')]/text()", input_type="F_XPATH", split_list={"lat:":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat:')]/text()", input_type="F_XPATH", split_list={"lng:":1, "}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//div[contains(@class,'ere-property-element')]//p//text()[contains(.,'Pets friendly')] ", input_type="F_XPATH", tf_item=True)
        
        item_loader.add_value("external_id", response.url.split("rental-")[1].split("-")[0])
        
        import dateparser
        available_date = response.xpath("//strong[contains(.,'date available')]/following-sibling::text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='media-gallery']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//strong[contains(.,'mobile')]/following-sibling::span/@data-phonenumber", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//strong[contains(.,'email')]/following-sibling::a/text()", input_type="F_XPATH")
        
        landlord_name = response.xpath("//h5[contains(@class,'card-title')]/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_value("landlord_name", "Seeto Real Estate")
            item_loader.add_value("landlord_phone", "02 8095 7799")

        rent_status = response.xpath("//p[contains(@class,'listing-info-price')]/text()[contains(.,'Deposit Taken')]").get()
        if rent_status:
            return

        yield item_loader.load_item()