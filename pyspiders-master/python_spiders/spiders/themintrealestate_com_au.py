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
    name = 'themintrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'themintrealestate.com.au',
        'content-length': '0',
        'accept': '*/*',
        'origin': 'https://themintrealestate.com.au',
        'referer': 'https://themintrealestate.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&doing_wp_cron=1615435704.6008739471435546875000&paged=1&extended=1&minprice=&maxprice=&minbeds=&maxbeds=&minbaths=&maxbaths=&cars=&type=&subcategory=&externalID=&minbuildarea=&maxbuildarea=&buildareaunit=&minlandarea=&maxlandarea=&landareaunit=&order=&search=',
        'accept-language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        start_url = "https://themintrealestate.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1615435704.6008739471435546875000&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bextended%5D%5Bvalue%5D=1&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5Bminbeds%5D%5Bvalue%5D=&query%5Bmaxbeds%5D%5Bvalue%5D=&query%5Bminbaths%5D%5Bvalue%5D=&query%5Bmaxbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5Btype%5D%5Bvalue%5D=&query%5Bsubcategory%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bminbuildarea%5D%5Bvalue%5D=&query%5Bmaxbuildarea%5D%5Bvalue%5D=&query%5Bbuildareaunit%5D%5Bvalue%5D=&query%5Bminlandarea%5D%5Bvalue%5D=&query%5Bmaxlandarea%5D%5Bvalue%5D=&query%5Blandareaunit%5D%5Bvalue%5D=&query%5Border%5D%5Bvalue%5D=&query%5Bsearch%5D%5Bvalue%5D="
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
            f_url = f"https://themintrealestate.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1615435704.6008739471435546875000&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Bextended%5D%5Bvalue%5D=1&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5Bminbeds%5D%5Bvalue%5D=&query%5Bmaxbeds%5D%5Bvalue%5D=&query%5Bminbaths%5D%5Bvalue%5D=&query%5Bmaxbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5Btype%5D%5Bvalue%5D=&query%5Bsubcategory%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bminbuildarea%5D%5Bvalue%5D=&query%5Bmaxbuildarea%5D%5Bvalue%5D=&query%5Bbuildareaunit%5D%5Bvalue%5D=&query%5Bminlandarea%5D%5Bvalue%5D=&query%5Bmaxlandarea%5D%5Bvalue%5D=&query%5Blandareaunit%5D%5Bvalue%5D=&query%5Border%5D%5Bvalue%5D=&query%5Bsearch%5D%5Bvalue%5D="
            yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page": page + 1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Themintrealestate_Com_PySpider_australia")

        external_id = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'property ID')]//parent::div/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//p[contains(@class,'address')]//text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//script[contains(.,'postAddress ')]/text()").get()
        if zipcode:
            zipcode = zipcode.split('postAddress = "')[1].split('"')[0].strip().split(" ")[-1]
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[contains(@class,'property-info-bar')]//p[contains(@class,'price')]//text()").get()
        if rent:
            if "-" in rent:
                rent = rent.split("$")[1].split("-")[0].strip()
            else:
                rent = rent.split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        deposit = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'bond')]//parent::div/text()").getall())
        if deposit:
            deposit = deposit.split("$")[1].strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'post-content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//p[contains(@class,'bed')]//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//p[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x.split("url('")[1].split("'")[0] for x in response.xpath("//div[contains(@class,'slides')]//@style[contains(.,'background')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'available')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'available')]//parent::div/text()").getall())
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//p[contains(@class,'car')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//section[contains(@class,'feature')]//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//h5[contains(@class,'post-title')]//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        dishwasher = response.xpath("//section[contains(@class,'feature')]//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        latitude_longitude = response.xpath("//script[contains(.,'Lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('postLat =')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('postLong =')[1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        landlord_name = response.xpath("//div[contains(@class,'agent')]//h5//a//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_email = response.xpath("//p[contains(@class,'staff-mobile')]//a[contains(@href,'mailto')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = "".join(response.xpath("//p[contains(@class,'staff-mobile')]//span[contains(@class,'phone')]//text()").getall())
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        
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