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
    name = 'timaltass_com'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings={
        "PROXY_ON":"True",
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://timaltass.com/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bpost_type%5D%5Btype%5D=equal&query%5Bcount%5D%5Bvalue%5D=20&query%5Bcount%5D%5Btype%5D=equal&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Borderby%5D%5Btype%5D=equal&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bmeta_key%5D%5Btype%5D=equal&query%5Bsold%5D%5Bvalue%5D=0&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bextended%5D%5Bvalue%5D=1&query%5Border%5D%5Bvalue%5D=priceMatch&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bpaged%5D%5Btype%5D=equal"},
	        
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        data = data["data"]["listings"]
        for item in data:
            if "Commercial" not in item["type"]:
                yield Request(item["url"], callback=self.populate_item, meta={"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "".join(response.xpath("//section[contains(@class,'description')]//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Timaltass_PySpider_australia")

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

        rent = response.xpath("//p[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split(" ")[0].replace("$","").strip()
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

        item_loader.add_value("landlord_name", "Tim Altass Real Estate")
        item_loader.add_value("landlord_phone", "07 3395 5002")
        item_loader.add_value("landlord_email", "rentals@timaltass.com")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "home" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "unit" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None