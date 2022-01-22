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
import re

class MySpider(Spider):
    name = 'burnham_com_au'   
    execution_type='testing'
    country='australia'
    locale='en'   
    
    post_url = "https://burnham.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={}"
    def start_requests(self):
        formdata = {
            "action": "get_posts",
            "query[post_type][value]": "listings",
            "query[count][value]": "99999",
            "query[orderby][value]": "meta_value",
            "query[meta_key][value]": "dateListed",
            "query[sold][value]": "0",
            "query[saleOrRental][value]": "Rental",
            "query[saleOrRental][type]": "equal",
            "query[paged][value]": "1",
        }
        yield FormRequest(
            url=self.post_url.format(1),
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["data"]
        for item in data["listings"]:
            status = item["status"]
            if status and ("available" not in status.lower() or "residential" not in item["type"].lower()):
                continue
            follow_url = item["url"]
            yield Request(follow_url, callback=self.populate_item)
        
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type = "".join(response.xpath("//h5[@class='single-post-title']/text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            p_type = "".join(response.xpath("//div[contains(@class,'post-content')]/p//text()").getall())
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
                item_loader.add_value("property_type", p_type)
            else:
                return
        item_loader.add_value("external_source", "Burnham_Com_PySpider_australia")
        
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)

        address = response.xpath("//p[contains(@class,'address')]/text()").get()
        if address:
            city = address.split(",")[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("address", address)
        
        rent = response.xpath("//p[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.split("$")[1].strip().split(" ")[0]
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "USD")

        room_count = response.xpath("//p[contains(@class,'bed')]/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//p[contains(@class,'bath')]/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//p[contains(@class,'car')]/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//strong[contains(.,'available')]/following-sibling::text()").get())
        available = response.xpath("//strong[contains(.,'available')]//text()").get()
        if available and "now" in available.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//strong[contains(.,'bond')]/following-sibling::text()").get()
        if deposit:
            deposit = deposit.split("$")[1].strip()
            item_loader.add_value("deposit", deposit)
        
        external_id = response.xpath("//strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        desc = " ".join(response.xpath("//div[contains(@class,'post-content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor" in desc.lower():
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1]
            not_list = ["a","seam","with","new", "hard"]
            status=True
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                item_loader.add_value("floor", floor)
        
        images = [x.split("url('")[1].split("')")[0] for x in response.xpath("//div[contains(@class,'slick-slides')]//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0]
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        landlord_name = response.xpath("//h5//a//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "BURNHAM")
        
        landlord_phone = response.xpath("//a[contains(@class,'phone-number__show brand-fg')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "03 9687 1344")

        landlord_email = response.xpath("//p[contains(@class,'email-text')]/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "town" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None