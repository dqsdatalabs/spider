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
import dateparser

class MySpider(Spider):
    name = 'maloneys_com'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://maloneys.com.au/rent/rental-listings/?streetsuburbRentals=&minpricer=&maxpricer=&listingtype%5B%5D=Apartment&qt=search&useSearchType=search&orderBy=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://maloneys.com.au/rent/rental-listings/?streetsuburbRentals=&minpricer=&maxpricer=&listingtype%5B%5D=House&qt=search&useSearchType=search&orderBy=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        property_type = response.meta.get("property_type")
        seen = False
        for item in response.xpath("//div[@class='property-box']/div/h1/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":property_type})
            seen = True

        if page == 2 or seen:
            url = f"https://maloneys.com.au/rent/rental-listings/page-{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Maloneys_Com_PySpider_australia")
        prop = response.meta.get("property_type")
        if prop is not None:
            item_loader.add_value("property_type",prop )
        else:
            item_loader.add_value("property_type",get_p_type_string(response.url))
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split(",")[-1])
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("room_count", "//ul[@class='listReset featureItems']/li[contains(.,'Bedroom')]/strong/text()")
        item_loader.add_xpath("bathroom_count", "//ul[@class='listReset featureItems']/li[contains(.,'Bathrooms')]/strong/text()")

        address = " ".join(response.xpath("//div[@class='pull-left']/p/text()").getall())
        if address:
            zipcode = address.split(",")[-1].strip().split(" ")[-1]
            city = address.split(",")[-1].strip().split(zipcode)[0]
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//div[@class='p-r']/h3/text()").extract_first()
        if rent:
            if 'weekly' in rent.lower():
                rent = "".join(filter(str.isnumeric, rent))
                item_loader.add_value("rent", str(int(rent) * 4))
            else:
                item_loader.add_value("rent", "".join(filter(str.isnumeric, rent)))
            item_loader.add_value("currency", 'AUD')

        item_loader.add_value("external_id", response.url.split(',')[-1].strip())

        deposit = response.xpath("//ul[@class='list-reset']/li[contains(.,'Bond')]/span/text()").extract_first()
        if deposit:
            dep =  deposit.replace(",","").strip()
            item_loader.add_value("deposit",dep)

        desc =  " ".join(response.xpath("//div[@class='mb-60']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date="".join(response.xpath("//ul[@class='list-reset']/li[contains(.,'Available')]//text()").getall())
        if available_date:
            date2 =  available_date.split("Available")[1].replace(":","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        parking =  " ".join(response.xpath("//ul[@class='listReset featureItems']/li[contains(.,'Garaging')]/strong/text()").extract())
        if parking:
            if parking !="0":
                item_loader.add_value("parking", True)

        images = [ x for x in response.xpath("//div[@class='slider slider-nav slider-sub']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images) 

        item_loader.add_value("landlord_name", "Moleneys Property")
        item_loader.add_value("landlord_phone", "02 6232 0100")
        item_loader.add_value("landlord_email", "maloneys@maloneys.com.au") 

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    else:
        return None