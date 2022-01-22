# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'domain_com_au'
    execution_type = 'testing'
    country = 'australia'
    locale = 'en'
    external_source = "Domain_PySpider_australia"
    custom_settings = { 
        "PROXY_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKitMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.domain.com.au/rent/?ptype=apartment&excludedeposittaken=1",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.domain.com.au/rent/?ptype=house&excludedeposittaken=1",
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    headers=self.headers,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)
        prop_type=response.meta.get('property_type')

        seen = False
        for item in response.xpath("//a[contains(@class,'address is-two-lines')]//@href").getall():
            follow_url = item
            yield Request(
                follow_url, 
                callback=self.populate_item,
                dont_filter=True, 
                headers=self.headers,
                meta={"property_type": response.meta.get('property_type')})
            seen = True

            if page == 2 or seen:
                url = f"https://www.domain.com.au/rent/?ptype={prop_type}&excludedeposittaken=1&page={page}"
                yield Request(
                    url,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'property_type': response.meta.get('property_type'), "page":page+1}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\u00e9","").replace("\u00e8","").replace("\u00b2","").replace("\u00a0",""))

        address = response.xpath("//span[@class='css-nn5f4o']/span/text()").get()
        if address:
            item_loader.add_value("address", address)

        description = "".join(response.xpath("(//div[@class='noscript-expander-content css-1ij7r2s'])[1]//p//text()").getall())
        if description:
            item_loader.add_value("description", description)

        room_count = response.xpath("(//span[contains(.,'Beds')]/preceding-sibling::text())[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count)  

        square_meters = response.xpath("(//span[contains(@class,'css-lvv8is')]/text()[contains(.,'m²')])[1]").get()
        if square_meters:
            square_meters =square_meters.split("m²")[0]
            if "." in square_meters:
               square_meters =square_meters.split(".")[0] 
               item_loader.add_value("square_meters", square_meters)  
            else:
                item_loader.add_value("square_meters", square_meters)  

        bathroom_count = response.xpath("(//span[contains(.,'Bath')]/preceding-sibling::text())[1]").get()
        if bathroom_count :
            item_loader.add_value("bathroom_count", bathroom_count)

        price = response.xpath("(//li[contains(.,'Bond')]//strong//text()[contains(.,'$')])[1]").get()
        if price:
            price= price.split("$")[1]
            item_loader.add_value("rent", price)
        else:
            price = "". join(response.xpath("//meta[@property='og:description']//@content").get())
            if price and "$" in price and "/" in price:
                price= price.split("$")[1].split("/")[0]
                if ".." in price:
                    price= price.split("..")[0]
                elif " -" in price:
                    price= price.split(" -")[0]
                elif "." in price:
                    price= price.split(".")[0]
                elif "," in price:
                    price= price.replace(",","")
                item_loader.add_value("rent", int(price)*4)
        # else:
        #     price = response.xpath("//div[@class='css-1texeil']//text()").get()
        #     if price and "pw" in price.lower():
        #         if "," in price:
        #             price= price.split("$")[1]
        #             price = price.split(" pw")[0].strip()
        #             price= price.replace(",","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         elif "." in price:
        #             price= price.split("$")[1]
        #             price = price.split(" pw")[0].strip()
        #             price= price.replace(".","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         else:
        #             price= price.split("$")[1]
        #             price = price.split(" pw")[0].strip()
        #             item_loader.add_value("rent", int(float(price)*4))
        #     elif price and "PW" in price.lower():
        #         if "," in price:
        #             price= price.split("$")[1]
        #             price = price.split(" PW")[0].strip()
        #             price= price.replace(",","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         elif "." in price:
        #             price= price.split("$")[1]
        #             price = price.split(" PW")[0].strip()
        #             price= price.replace(".","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         else:
        #             price= price.split("$")[1]
        #             price = price.split(" PW")[0].strip()
        #             item_loader.add_value("rent", int(float(price)*4))
        #     elif price and "per week" in price.lower():
        #         if "," in price:
        #             price= price.split("$")[1]
        #             price = price.split(" per week")[0].strip()
        #             price= price.replace(",","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         elif "." in price:
        #             price= price.split("$")[1]
        #             price = price.split(" per week")[0].strip()
        #             price= price.replace(".","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         else:
        #             price= price.split("$")[1]
        #             price = price.split(" per week")[0].strip()
        #             item_loader.add_value("rent", int(float(price)*4))

        #     elif price and "Per Week" in price.lower():
        #         if "," in price:
        #             price= price.split("$")[1]
        #             price = price.split(" Per Week")[0].strip()
        #             price= price.replace(",","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         elif "." in price:
        #             price= price.split("$")[1]
        #             price = price.split(" Per Week")[0].strip()
        #             price= price.replace(".","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         else:
        #             price= price.split("$")[1]
        #             price = price.split(" Per Week")[0].strip()
        #             item_loader.add_value("rent", int(float(price)*4))

        #     elif price and "per Week" in price.lower():
        #         if "," in price:
        #             price= price.split("$")[1]
        #             price = price.split(" per Week")[0].strip()
        #             price= price.replace(",","")
        #             item_loader.add_value("rent",int(float(price)*4))
        #         elif "." in price:
        #             price= price.split("$")[1]
        #             price = price.split(" per Week")[0].strip()
        #             price= price.replace(".","")
        #             item_loader.add_value("rent", int(float(price)*4))
        #         else:
        #             price= price.split("$")[1]
        #             price = price.split(" per Week")[0].strip()
        #             item_loader.add_value("rent", int(float(price)*4))
        item_loader.add_value("currency", "AUD")

        latitude_longitude = response.xpath(
            "//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                '"latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split(
                '"longitude":')[1].split(',')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
                  
        images ="".join (response.xpath("//script[contains(.,'thumbnail')]//text()").get())
        images = images.split('thumbnail":"')
        for i in range(1,len(images)):
            item_loader.add_value("images", images[i].split('",')[0])

        landlord_name = "".join(response.xpath("//img[@class='css-f45itd']//@alt").get())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//script[contains(.,'thumbnail')]//text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split('"phone":"')[1].split('","')[0]
            item_loader.add_value("landlord_phone", landlord_phone)
                    
        yield item_loader.load_item()