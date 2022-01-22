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
    name = 'vicprop_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'vicprop.com.au',
        'content-length': '0',
        'accept': '*/*',
        'origin': 'https://vicprop.com.au',
        'referer': 'https://vicprop.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&paged=1&type=residential&order=dateListed-desc&beds=&baths=&cars=&externalID=&minprice=&maxprice=&underAgency=',
        'accept-language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        start_url = "https://vicprop.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1616496949.8348960876464843750000&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bpaged%5D%5Btype%5D=equal"
        yield FormRequest(start_url, headers=self.headers, callback=self.parse)

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
            f_url = f"https://vicprop.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1616496949.8348960876464843750000&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Bpaged%5D%5Btype%5D=equal"            
            yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//h5[contains(@class,'single-post-title')]//text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            property_type = "".join(response.xpath("//div[contains(@class,'post-content')]//text()").getall())
            if get_p_type_string(property_type):
                item_loader.add_value("property_type", get_p_type_string(property_type))
            else:
                if get_p_type_string(response.url):
                    item_loader.add_value("property_type", get_p_type_string(response.url))
                else:
                    print(response.url)
         
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Vicprop_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[contains(@class,'address')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(@class,'address')]/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[contains(@class,'bed')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(@class,'bath')]/text()", input_type="F_XPATH", get_num=True)
        
        item = response.meta.get('item')
        item_loader.add_value("title", item["title"])
        item_loader.add_value("latitude", item["lat"])
        item_loader.add_value("longitude", item["long"])
        rent = item["price"]
        if rent:
            rent = rent.split("$")[-1].lower().split('p')[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
        if "floor" in item["post_content"]:
            floor = item["post_content"].split("floor")[0].strip().split(" ")[-1]
            if "Polis" not in floor and "timber" not in floor:
                item_loader.add_value("floor", floor)
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='b-description__text']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'ID')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'bond')]/following-sibling::text()", input_type="F_XPATH", split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'available')]/text()", input_type="F_XPATH", split_list={"available":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[contains(@class,'car')]/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='media-gallery']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='media-gallery']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h5[contains(@class,'card-title')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span[contains(@class,'phone-number')]/a/text()", input_type="F_XPATH")

        land_email = response.xpath("//p/a[contains(@href,'mail')]/@href[contains(.,'@')]").get()
        if land_email:
            item_loader.add_value("landlord_email", land_email.split(":")[1])

        if not item_loader.get_collected_values("available_date"):
            ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'available')]/following-sibling::text()", input_type="F_XPATH")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "residential" in p_type_string.lower() or "residence" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None