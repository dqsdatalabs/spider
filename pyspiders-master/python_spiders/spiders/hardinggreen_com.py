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
    name = 'hardinggreen_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    external_source='Hardinggreen_PySpider_united_kingdom_en'
    start_urls = ["https://hardinggreen.com/properties/?pquery=1&pview=grid&searchterm=&max_price=No+max&min_price=No+min&min_beds=No+min&type=rent"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='shadow-hg posrel']/@href").getall():
            follow_url = response.urljoin(item)
            # status = item.xpath("./span[contains(@class,'red')]").get()
            # if status:
            #     continue
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:
            headers={
                "authority": "hardinggreen.com",
                "path": "/properties/page/{page}/?pquery=1&pview=grid&searchterm&max_price=No%20max&min_price=No%20min&min_beds=No%20min&type=rent",
                "scheme": "https",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "cache-control": "max-age=0",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
            }
            follow_url = f"https://hardinggreen.com/properties/page/{page}/?pquery=1"
            yield Request(follow_url, headers=headers,callback=self.parse,meta={"page": page + 1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("(//div[contains(@class,'mt-6 flex items-center space-x-6')]//span//text())[1]").get()
        if "for sale" not in status.lower():

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)

            meta_title = response.xpath("//head/title/text()").get()
            studio ="".join (response.xpath("//div[@class='row content-row']/div[contains(.,'Studio')]").extract())
            if "Studio" in studio:
                item_loader.add_value("property_type", "studio")
            elif meta_title and ("apartment" in meta_title.lower() or "flat" in meta_title.lower() or "maisonette" in meta_title.lower()):
                item_loader.add_value("property_type", "apartment")
            elif meta_title and "house" in meta_title.lower():
                item_loader.add_value("property_type", "house")
            elif meta_title and "studio" in meta_title.lower():
                item_loader.add_value("property_type", "studio")
            else:
                return

            title = response.xpath("//title/text()").get()
            if title:
                item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))

            externalid=response.url
            if externalid:
                item_loader.add_value("external_id",externalid.split("property/")[-1].split("/")[0])
            
            address = "".join(response.xpath("//h1/text()").getall()).replace("EPC","")
            if address:
                zipcode = address.strip().split(" ")[-1]
                city = address.strip().split(zipcode)[0].strip().strip(",").split(",")[-1]
                item_loader.add_value("address", address.strip())
                item_loader.add_value("city", city.strip())
                item_loader.add_value("zipcode", zipcode)
                
            rent = "".join(response.xpath("(//span[@class='block font-semibold uppercase text-[13px] md:text-[18px] tracking-[0.3rem] text-blue-dark-title md:text-white']//text())[1]").getall())
            if rent:
                price = rent.split("£")[1].split(",")[0]
                item_loader.add_value("rent", price)
                
            item_loader.add_value("currency", "GBP")
            
            room_count = response.xpath("(//span[@class='block uppercase font-sans font-bold text-[13px] md:text-[15px] tracking-[0.3rem] text-blue-dark-title']//text())[1]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])
              
            bathroom_count = response.xpath("(//span[@class='block uppercase font-sans font-bold text-[13px] md:text-[15px] tracking-[0.3rem] text-blue-dark-title']//text())[2]").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
            
            desc = "".join(response.xpath("//div/h3[contains(.,'Desc')]/../p//text()").getall())
            if desc:
                item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            dontallow="".join(response.xpath("//div/h3[contains(.,'Desc')]/../p//text()").getall())
            if dontallow and "Short-Lets" in dontallow:
                return 

            sqm = False
            square_meters = response.xpath("//div/h3[contains(.,'Features')]/../div/div[contains(.,'sqft')]/text()").get()
            if square_meters:
                square_meters = square_meters.split("sqft")[0].strip().split(" ")[-1]
                sqm = sqm = str(int(int(square_meters)* 0.09290304))
            elif "sqm" in desc:
                sqm = desc.split("sqm")[0].strip().split(" ")[-1]
            elif "sqft" in desc.lower():
                sq_m = desc.lower().split("sqft")[0].strip().split(" ")[-1]
                sqm = str(int(int(sq_m)* 0.09290304))
            elif "sq.ft" in desc.lower():
                sq_m = desc.lower().split("sq.ft")[0].strip().split(" ")[-1].replace("(","").replace(",","")
                sqm = str(int(int(sq_m)* 0.09290304))
            elif "square" in desc.lower():
                sq_ft = desc.lower().split("square")[0].strip().split(" ")[-1]
                if sq_ft.isdigit():
                    sqm = str(int(int(sq_ft)* 0.09290304))
            if sqm:
                item_loader.add_value("square_meters", sqm)      
                    
            lat_lng = response.xpath("//script[contains(.,'LatLng')]//text()").get()
            if lat_lng:
                lat = lat_lng.split('google.maps.LatLng(')[1].split(',')[0]
                lng = lat_lng.split('google.maps.LatLng(')[1].split(',')[1].split(');')[0].strip()
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
            
            images = [ x for x in response.xpath("//div[contains(@class,'swiper-slide')]//img//@src").getall()]
            if images:
                item_loader.add_value("images", images)
            
            floor_plan_images = response.xpath("//div[@class='fplowres']/a/img/@src").get()
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
            
            unfurnished = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'Unfurnished') or contains(.,'unfurnished')]/text()"
                ).get()
            furnished = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'Furnished') or contains(.,'furnished')]/text()"
                ).get()
            if unfurnished:
                item_loader.add_value("furnished", False)
            elif furnished:
                item_loader.add_value("furnished", True)
            
            floor = response.xpath("//div/h3[contains(.,'Features')]/../div/div[contains(.,'Floor')]/text()").get()
            if floor:
                floor = floor.split("Floor")[0].strip()
                floor2 = floor.replace("th","").replace("st","").replace("rd","").replace("nd","")
                if floor2.isdigit():
                    item_loader.add_value("floor",floor2)
            
            energy_label = response.xpath("//div/h3[contains(.,'Features')]/../div/div[contains(.,'EPC')]/text()").get()
            if energy_label:
                item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
            
            parking = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'Parking') or contains(.,'parking')]/text()"
                ).get()
            if parking:
                item_loader.add_value("parking", True)

            
            balcony = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'Balcon') or contains(.,'balcon')]/text()"
                ).get()
            if balcony:
                item_loader.add_value("balcony", True)
            
            elevator = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'Lift') or contains(.,'lift')]/text()"
                ).get()
            if elevator:
                item_loader.add_value("elevator", True)
            
            terrace = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'terrace') or contains(.,'Terrace')]/text()"
                ).get()
            if terrace:
                item_loader.add_value("terrace", True)
            
            name = response.xpath("//span[contains(@class,'name')]/text()").get()
            if name:
                item_loader.add_value("landlord_name", name)
            else:
                item_loader.add_value("landlord_name", "Harding Green")

            name = response.xpath("(//h3[@class='font-serif text-[20px] md:text-[25px] leading-[28px] md:leading-[35px] text-blue-dark-title']//text())[1]").get()
            if name:
                item_loader.add_value("landlord_name", name)
            else:
                item_loader.add_value("landlord_phone", "0203 3751 970")

            phone = response.xpath("(//a[@class='font-sans font-light text-[16px] lg:text-[18px] text-blue-dark-body flex items-center justify-center md:justify-start space-x-3 mt-3 object-link']//span//text())[7]").get()
            if phone:
                item_loader.add_value("landlord_phone", phone)
            else:
                item_loader.add_value("landlord_phone", "0203 3751 970")
                
            email = response.xpath("(//div[@class='relative font-sans font-light text-[15px] lg:text-[18px] lg:leading-[21px] text-blue-dark-body flex items-center overflow-hidden w-full']//span//text())[1]").get()
            if email:
                item_loader.add_value("landlord_email", email)
            else:
                item_loader.add_value("landlord_email", "info@hardinggreen.com")
                
            features = response.xpath(
                "//div/h3[contains(.,'Features')]/../div/div[contains(.,'Short Let') or contains(.,'Short-Let')]/text()"
                ).getall()
            if features:
                return
            else:
                yield item_loader.load_item()

