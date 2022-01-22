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
class MySpider(Spider):
    name = 'rhodesrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'rhodesrealty.com.au',
        'content-length': '0',
        'accept': '*/*',
        'origin': 'https://rhodesrealty.com.au',
        'referer': 'https://rhodesrealty.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&doing_wp_cron=1612518040.5500459671020507812500&paged=1&extended=1&minprice=&maxprice=&minbeds=&maxbeds=&baths=&cars=&type=residential&externalID=&subcategory=&landsize=&order=dateListed-desc',
        'accept-language': 'tr,en;q=0.9',
        'cookie': '_ga=GA1.3.324335216.1612518029; _gid=GA1.3.1380300115.1612518029; _fbp=fb.2.1612518029739.1923029691; _gcl_au=1.1.1421927650.1612518032'
    }

    def start_requests(self):
        start_url = "https://rhodesrealty.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1612518040.5500459671020507812500&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bpaged%5D%5Btype%5D=equal&query%5Bextended%5D%5Bvalue%5D=1&query%5Bextended%5D%5Btype%5D=equal&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal"
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
                else: yield Request(follow_url, callback=self.populate_item, meta={"item":item})
        if page == 2 or seen: 
            f_url = f"https://rhodesrealty.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1612518040.5500459671020507812500&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Bpaged%5D%5Btype%5D=equal&query%5Bextended%5D%5Bvalue%5D=1&query%5Bextended%5D%5Btype%5D=equal&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal"
            yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if response.meta.get("property_type"):
            item_loader.add_value("property_type", response.meta["property_type"])
        else:
            prop_type = response.xpath("//h5[contains(@class,'single-post-title')]//text()[not(contains(.,'Car space'))]").get()
            if prop_type:
                if get_p_type_string(prop_type):
                    item_loader.add_value("property_type", get_p_type_string(prop_type))
                else:
                    prop_type = "".join(response.xpath("//div[@class='b-description__text']//text()").getall())
                    if get_p_type_string(prop_type):
                        item_loader.add_value("property_type", get_p_type_string(prop_type))
                    else:
                        prop_type = response.url
                        if get_p_type_string(prop_type):
                            item_loader.add_value("property_type", get_p_type_string(prop_type))
                        else: return                                          
            else: return

        item_loader.add_value("external_link", response.url)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Rhodesrealty_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[contains(@class,'address')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(@class,'address')]/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[contains(@class,'bed')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(@class,'bath')]/text()", input_type="F_XPATH", get_num=True)
        
        item = response.meta.get('item')
        item_loader.add_value("latitude", item["lat"])
        item_loader.add_value("longitude", item["long"])

        rent = item["price"]
        if "DEPOSIT TAKEN" in rent.upper():
            return
        if rent:
            rent = rent.split("$")[-1].lower().split('p')[0].strip().split(" ")[0].replace(',', '').split("-")[0].strip()
            if rent:
                item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
    
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='b-description__text']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'ID')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH", replace_list={"|":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'bond')]/following-sibling::text()", input_type="F_XPATH", split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'available')]/text() | //div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'available')]/following-sibling::text()", input_type="M_XPATH", split_list={"available":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[contains(@class,'car')]/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//section[@id='single-listings-content']//li[.='Balcony']/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//section[@id='single-listings-content']//li[.='Dishwasher']/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//section[@id='single-listings-content']//li[contains(.,'Pool ')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='media-gallery']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h5[contains(@class,'card-title')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span[contains(@class,'phone-number')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//p/a[contains(@href,'mail')]/text()[contains(.,'@')]", input_type="F_XPATH")

        zipcode = response.xpath("//script[contains(.,'postAddress')]//text()").get()
        if zipcode:
            zipcode = zipcode.split('postAddress = "')[1].split('"')[0].split(" ")[-1].strip()
            item_loader.add_value("zipcode", zipcode)

        pets = "".join(response.xpath("//div[@class='section-body post-content']/p/text()[contains(.,'pets')]").getall())
        if pets:
            if "no" in pets.lower():
                item_loader.add_value("pets_allowed", False)
            elif "yes" in pets.lower():
                item_loader.add_value("pets_allowed", True)        
        furnished = response.xpath("//section[@id='single-listings-content']//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)  
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "residential" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"    
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None