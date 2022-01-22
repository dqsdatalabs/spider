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
    name = 'propertyheads_com'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "PROXY_US_ON" : True,
        "CONCURRENT_REQUESTS": 3,
        "COOKIES_ENABLED": False,
        "RETRY_TIMES": 3,
        # "DOWNLOAD_DELAY": 5,

    } 
    download_timeout = 200

    def start_requests(self):
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36', 
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 
                    'accept-language': 'en', 'Accept-Encoding': 'gzip, deflate'
        }
        start_urls = [
            {
                "url": [
                    "https://propertyheads.com/index.php?p=0&module=prp_search&trans_type_id=2&location_id=&location=&distance=0&category=16%2C3%2C4%2C5%2C6%2C1%2C2&minprice=0&maxprice=0&minbeds=0&maxbeds=0&keywords=&added=0&sort=1&save=&exc_sold=1&location=&handler=search&action=perform&search_type=properties&sub_action=do_search",
                ],
                "property_type": "house","type":"16%2C3%2C4%2C5%2C6%2C1%2C2"
            },
	        {
                "url": [
                    "https://propertyheads.com/index.php?p=0&module=prp_search&trans_type_id=2&location_id=&location=&distance=0&category=7%2C8%2C9%2C10%2C11&minprice=0&maxprice=0&minbeds=0&maxbeds=0&keywords=&added=0&sort=1&save=&exc_sold=1&location=&handler=search&action=perform&search_type=properties&sub_action=do_search"
                ],
                "property_type": "apartment","type":"7%2C8%2C9%2C10%2C11"
            }
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=headers,
                    meta={'property_type': url.get('property_type'),'type':url.get('type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        # try:
        #     data = json.loads(response.body)["properties"]["cards"]
        # except: data = ""
        # page = response.meta.get('page', 2)

        # seen = False
        # if data:
        #     for item in data:
        #         yield Request(item["link"], callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        #         seen = True

        # if page == 2 or seen:
        #     url = response.url.replace(f"p={page-1}", f"p={page}")
        #     yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//div[@class='title-wrapper']/a"):
            status = "".join(item.xpath(".//text()").getall())
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen=True
        
        if page == 1 or seen:
            
            p_url = f"https://propertyheads.com/index.php?p={page}&module=prp_search&trans_type_id=2&location_id=&location=&distance=0&category={type}&minprice=0&maxprice=0&minbeds=0&maxbeds=0&keywords=&added=0&sort=1&save=&exc_sold=1&location=&handler=search&action=perform&search_type=properties&sub_action=do_search"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "type":response.meta["type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status=response.xpath("//h1[@class='property-title']//text()").get()
        if status and ("restaurant" in status.lower() or "parking" in status.lower()):
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Propertyheads_PySpider_united_kingdom")

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        city = "".join(response.xpath("//div[@class='breadcrumbs']//a[contains(.,'Bedrooms')]//following-sibling::a//text()").get())
        if city:
            city = "".join(city.split(",")[-2:-1])
            item_loader.add_value("city", city)
        zipcode = "".join(response.xpath("//div[@class='breadcrumbs']//a[contains(.,'Bedrooms')]//following-sibling::a//text()").get())
        if zipcode: 
            zipcode= "".join(zipcode.split(",")[-1:])
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//div[contains(@class,'floor-area')]//span//text()").get()
        if square_meters and ":" in square_meters:
            square_meters = square_meters.split(":")[-1].strip().split("m")[0].split(",")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        control=response.xpath("//h3[contains(@class,'price')]/span/text()").get()
        if control and "annual" in control.lower():
            rent = response.xpath("//h3[contains(@class,'price')]/text()").get()
            if rent:
                rent = rent.strip().replace("£","").replace(",","")
                rent=int(float(rent) / 12)
                item_loader.add_value("rent", rent)
        elif control and "monthly" in control.lower():
            rent = response.xpath("//h3[contains(@class,'price')]/text()").get()
            if rent:
                rent = rent.strip().replace("£","").replace(",","")
                # rent=int(float(rent) / 30)
                item_loader.add_value("rent", rent)
        elif control and "week" in control.lower():
            rent = response.xpath("//h3[contains(@class,'price')]/text()").get()
            if rent:
                rent = rent.strip().replace("£","").replace(",","")
                rent=int(float(rent) *4)
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//span[contains(@class,'description')]//text()[not(contains(.,'$'))]").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        deposit = response.xpath("//span[@class='description']//text()[contains(.,'Deposit:')]").get()
        if deposit:
            deposit = deposit.split("£")[1].split(" ")[0]
            if "," in deposit:
                deposit = deposit.replace(",","")
                item_loader.add_value("deposit", deposit)
            else:
                item_loader.add_value("deposit", deposit)

        room_count = response.xpath("//div[@class='breadcrumbs']//a//text()[contains(.,'Bedroom')]").get()
        if room_count :
            room_count = room_count.split("Bedrooms")[0]
            item_loader.add_value("room_count", room_count)


        images = [x for x in response.xpath("//div[contains(@class,'my-gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'transform([')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('transform([')[1].split(",")[1].split(']')[0]
            longitude = latitude_longitude.split('transform([')[1].split(',')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agent')]//div[contains(@class,'label')]//a//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[contains(@class,'agent')]//a[contains(@href,'tel')]//span//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        if "properties/" in response.url:
            item_loader.add_value("external_id", response.url.split("properties/")[1].split("/")[0])
            yield item_loader.load_item()
