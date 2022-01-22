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
    name = 'andrewreeves_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
        "PROXY_TR_ON": True,
        "HTTPCACHE_ENABLED": False
    }
    headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            # "accept-language": "en-US,en;q=0.9,tr;q=0.8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://andrewreeves.co.uk/search-results/page/1/?department=residential-lettings&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type=22&minimum_floor_area&maximum_floor_area&commercial_property_type&address_keyword",
                ],
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='bootstrap']/ul/li"):
            follow_url = response.urljoin(item.xpath("./div[@class='details']/h3/a/@href").get())
            let_agreed = item.xpath(".//a/div[.='Let Agreed']").get()
            if let_agreed:
                continue
            yield Request(follow_url,headers=self.headers, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_url = response.xpath("//div[@class='propertyhive-pagination']//a[@class='next page-numbers']/@href").extract_first()
        if next_url:
            yield Request(
                next_url,
                headers=self.headers,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
        
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # let_agreed = response.xpath("//div[@class='flag flag-let-agreed']/text()").get()
        # if let_agreed and 'Let Agreed' in let_agreed:
        #     return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Andrewreeves_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_id", response.url.split("_")[1].strip("/"))

        item_loader.add_xpath("title", "//h1[contains(@class,'property_title ')]/text()")
        item_loader.add_xpath("address", "//h1[contains(@class,'property_title ')]/text()")
        item_loader.add_value("city", "London")
        zipcode=item_loader.get_output_value("title")
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1].strip())

        rent = "".join(response.xpath("//div[@class='price']/text()").getall()).strip()
        if rent:
            term = rent.strip().split(' ')[-1]
            if 'pw' in term.lower() or 'per week' in term.lower():
                item_loader.add_value("rent", str(int(rent.strip().split(' ')[0].split('£')[-1].split('.')[0].replace(',', '').strip()) * 4))
                item_loader.add_value("currency","GBP")
            else:
                item_loader.add_value("rent", rent.strip().split(' ')[0].split('£')[-1].replace(',', '').strip())
                item_loader.add_value("currency","GBP")

        room = response.xpath("//ul[@class='prodesk']/li/text()[contains(.,'Bedrooms')]").extract_first()
        if room:
            item_loader.add_value("room_count",room.strip().replace("Bedrooms",""))

        floor = response.xpath("//div[@class='features']/ul/li/text()[contains(.,'Floor')]").extract_first()
        if floor:
            item_loader.add_value("floor",floor.replace("Floor",""))
        else:
            floor_s = " ".join(response.xpath("//div[@id='ph-description']/div[@class='summary']//text()[contains(.,'floor')]").extract())
            if floor_s:
                floor = floor_s.split("floor")[0].strip().split(" ")[-1]
                item_loader.add_value("floor",floor)

        square_meters = " ".join(response.xpath("//div[@class='row']/div/p/text()[contains(.,'square feet')]").extract())
        if square_meters:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)


        label = response.xpath("//div[@class='features']/ul/li/text()[contains(.,'EPC')]").extract_first()
        if label:
            if label.split('-')[-1].strip() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label",label.split("-")[1])


        desc = " ".join(response.xpath("//div[@class='row']/div/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc)

        images=[x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        bathroom_count = " ".join(response.xpath("//ul[@class='prodesk']/li/text()[contains(.,'Bathrooms')]").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split("Bathrooms")[0])

        parking = "".join(response.xpath("//div[@class='features']/ul/li/text()[contains(.,'Parking')]").extract())
        if parking:
            item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//div[@class='features']/ul/li/text()[contains(.,'Balcony')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_xpath("latitude","//p[@id='lat_val']/text()")
        item_loader.add_xpath("longitude","//p[@id='lon_val']/text()")

        item_loader.add_value("landlord_name", "ANDREW REEVES")
        item_loader.add_value("landlord_phone", "0203 993 7504")


        # else:
        #     pw_rent = "".join(response.xpath("substring-after(//h2/text()[contains(.,'PW')],'£') ").extract())
        #     if pw_rent:
        #         pw = pw_rent.split("PW")[0].strip()
        #         item_loader.add_value("rent",int(pw)*4 )

        yield item_loader.load_item()
