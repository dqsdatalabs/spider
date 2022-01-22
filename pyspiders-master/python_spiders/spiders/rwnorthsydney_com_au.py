# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'rwnorthsydney_com_au'
    execution_type='testing'
    country='australia' 
    locale='en'
    external_source="Rwnorthsydney_Com_PySpider_australia"
    start_urls = ["https://www.rwnorthsydney.com.au/property-for-lease.html"]

    formdata = {
        "act": "act_fgxml",
        "31[offset]": "6",
        "31[perpage]": "6",
        "SalesStageID[0]": "LISTED_PT",
        "SalesStageID[1]": "LISTED_LEASE",
        "SalesStageID[2]": "LISTED_AUCTION",
        "SalesStageID[3]": "DEPOSIT",
        "SalesStageID[4]": "BOND",
        "SalesStageID[5]": "EXCHANGED_UNREPORTED",
        "SalesCategoryID[0]": "RESIDENTIAL_LEASE",
        "SalesCategoryID[1]": "RURAL_LEASE",
        "SalesCategoryID[2]": "COMMERCIAL_LEASE",
        "Address": "",
        "RegionID": "",
        "require": "0",
        "fgpid": "31",
        "ajax": "1",
    }

    def start_requests(self):
        yield FormRequest(
            url=self.start_urls[0],
            formdata=self.formdata,
            callback=self.parse,
        )
    


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 6)
        seen = False
        for item in response.xpath("//row/url/text()").getall():
            follow_url = "https://rwnorthsydney.com.au" + item
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            url = "https://marrickville.randw.com.au/property.html"
            self.formdata["31[offset]"] = str(page)
            yield FormRequest(url, dont_filter=True, formdata=self.formdata, callback=self.parse, meta={"page": page+6,})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//title/text()").get()
        if property_type:
            if get_p_type_string(property_type.split('-')[0]): item_loader.add_value("property_type", get_p_type_string(property_type.split('-')[0]))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        # item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title",title)

        address = response.xpath("//h1[@class='heading']//text()").get()
        if address:
            item_loader.add_value("address", address)
        rent = response.xpath("//div[contains(.,'PER WEEK')]/text()").get()
        if rent:
            try:
                rent = rent.split("$")[1].lower().split("per")[0]
                item_loader.add_value("rent", int(float(rent))*4)
            except:
                rent = rent.split(".")[0].strip().replace(",","")
                if rent.isdigit():
                    item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//div[contains(.,'Bond')]/text()").get()
        if deposit:
            deposit = deposit.split("$")[-1].split(".")[0].replace(",","").strip()
            item_loader.add_value("deposit", deposit)

        
        description = " ".join(response.xpath("//div[@class='descview-inner']/text()").getall())
        if description:
            item_loader.add_value("description",description)

        room_count=response.xpath("//div[contains(.,'bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split("bed")[0].strip())
        bathroom_count=response.xpath("//div[contains(.,'bed')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("bath")[0].split("|")[-1].strip())
        
        images = [x for x in response.xpath("//div[@class='image image-link image-fluid image-property']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("//div[contains(@class,'box-light-inner')]//h3//text()").get()
        item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[contains(@class,'box-light-inner')]//a//text()").get()
        item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_email", "mail@rwns.com.au")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "duplex" in p_type_string.lower()):
        return "house"
    else:
        return None