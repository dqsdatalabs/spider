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
    name = 'bowgroup_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_url = "https://bowgroup.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bpaged%5D%5Btype%5D=equal&query%5Bextended%5D%5Bvalue%5D=1&query%5Bextended%5D%5Btype%5D=equal&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal"
        yield FormRequest(start_url, callback=self.parse)

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
            f_url = f"https://bowgroup.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Border%5D%5Bvalue%5D=dateListed-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Bpaged%5D%5Btype%5D=equal&query%5Bextended%5D%5Bvalue%5D=1&query%5Bextended%5D%5Btype%5D=equal&query%5Btype%5D%5Bvalue%5D=residential&query%5Btype%5D%5Btype%5D=equal"
            yield FormRequest(f_url, callback=self.parse, meta={"page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bowgroup_Com_PySpider_australia")

        external_id = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'ID')]//parent::div/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        item = response.meta.get("item")
        latitude = item["lat"]
        longitude = item["long"]
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)

        title= response.xpath("//div[contains(@class,'section-heading')]//following-sibling::h5//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//p[contains(@class,'address')]//text()").get()
        if address:
            city= address.split(",")[-1]
            item_loader.add_value("address",address)
            item_loader.add_value("city", city)

        zipcode = response.xpath("//script[contains(.,'postAddress')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split('postAddress = "')[1].split('"')[0].strip().split(" ")[-1].strip())

        rent = response.xpath("//p[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("$")[1].split(" ")[0]
            if "," not in rent:
                item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")

        deposit = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'bond')]//parent::div/text()").getall())
        if deposit:
            deposit = deposit.strip().split("$")[1]
            item_loader.add_value("deposit", deposit)

        room_count = response.xpath("//p[contains(@class,'bed')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//p[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//p[contains(@class,'car')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        desc = " ".join(response.xpath("//div[contains(@class,'post-content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        furnished = response.xpath("//h5[contains(@class,'post-title')]//text()").get()
        if furnished and "furnished" in furnished.lower() and "unfurnished" not in furnished.lower():
            item_loader.add_value("furnished", True)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'available')]//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = "".join(response.xpath("//div[contains(@class,'section-header')]//strong[contains(.,'available')]//parent::div/text()").getall())
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[contains(@class,'listing-media-gallery-slider')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = "".join(response.xpath("//div[contains(@class,'heading')][contains(.,'Agent Detail')]//parent::div//following-sibling::div//h5/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_email = response.xpath("//p[contains(@class,'mobile')]//a[contains(@href,'mail')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@class,'heading')][contains(.,'Agent Detail')]//parent::div//following-sibling::div//span[contains(@class,'phone-number')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
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