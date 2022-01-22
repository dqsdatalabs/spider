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
    name = 'candmproperties_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Candmproperties_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.zoopla.co.uk/to-rent/branch/c-and-m-property-management-middlesbrough-52870/?branch_id=52870&include_shared_accommodation=false&price_frequency=per_month&property_type=flats&results_sort=newest_listings&search_source=refine",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.zoopla.co.uk/to-rent/branch/c-and-m-property-management-middlesbrough-52870/?branch_id=52870&include_shared_accommodation=false&price_frequency=per_month&property_type=houses&results_sort=newest_listings&search_source=refine"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')} 
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='photo-hover']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        available_date = response.xpath("//text()[contains(.,'Available from')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(" from")[1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        parking = response.xpath("//li//text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True) 
        external_id = response.url.split("/")[-2]
        f_id = f"//script[contains(.,'listingId\":{external_id}')]/text()"
        script = response.xpath(f_id).get()
        if script:
            data = json.loads(script)

            try:
                data = data["props"]["pageProps"]["data"]
                item_loader.add_value("external_id", external_id)
                item_loader.add_value("description", data["listing"]["detailedDescription"])
                
            except:
                pass
        imgg=[]
        images=script
        if images:
            images=images.split("propertyImage")[-1].split("section")[0]
            count=images.count("filename")
            print(images)
            sayi=0
            while sayi<=count:
                index=images.find('filename')
                imag=images[index:]
                img=imag.split("}")[0]
                
                imagesindex=imag.find('}')
                images=images[imagesindex:]
                sayi=sayi+1
                imgg.append(img.split(":")[-1].replace('"',""))

            images=[f"https://lid.zoocdn.com/u/2400/1800/{x}" for x in imgg]
            if images:
                item_loader.add_value("images",images)

        room_count=response.xpath("//span[@data-testid='beds-label']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        bathroom_count=response.xpath("//span[@data-testid='baths-label']/text()").get() 
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0])
        latitude=response.xpath("//script[contains(.,'GeoCoordinates')]/text()").get()
        if latitude:
            latitude=latitude.split("GeoCoordinates")[-1].split("latitude")[1].split(",")[0]
            item_loader.add_value("latitude",latitude.split(":")[-1].replace("\n","").replace("\t",""))
        longitude=response.xpath("//script[contains(.,'GeoCoordinates')]/text()").get()
        if longitude:
            longitude=longitude.split("GeoCoordinates")[-1].split("longitude")[1].split("}")[0]
            item_loader.add_value("longitude",longitude.split(":")[-1].replace("\n","").replace("\t",""))

        rent=response.xpath("//span[@data-testid='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(",","").split("£")[1].split(" ")[0])
        item_loader.add_value("currency","GBP")
        address=response.xpath("//span[@data-testid='address-label']/text()").get()
        if address:
            item_loader.add_value("address",address)
            item_loader.add_value("city",address.split(",")[1])
            item_loader.add_value("zipcode",address.split(",")[1].split(" ")[-1])

        # phone=response.xpath("//a[@data-testid='agent-dynamic-phone-cta']/@href").get()
        # if phone:
        #     item_loader.add_value("landlord_phone",phone)
        item_loader.add_value("landlord_phone","+44 1642 200037 ")
        name=response.xpath("//p[@class='css-1g2z706-Text-AgentName e1swwt8d3']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)

        yield item_loader.load_item()