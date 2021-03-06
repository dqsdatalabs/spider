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
    name = 'cb_com_trofficesdetail3BAYourportfolio'
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=65&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=9&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=3&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=68&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=2&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=4&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=113&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=5&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                ],
                "property_type" : "house"
            },
        ]# LEVEL 1
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='title h5']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})
            seen = True
        
        
        if page == 2 or seen:
            url = response.url.split("&pager_p")[0] + f"&pager_p={page}"
            yield Request(
                url=url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cbcomtrofficesdetail3BAYourportfolio_PySpider_" + self.country + "_" + self.locale)
        external_id = "".join(response.xpath("//div[@class='feature-item']/text()[contains(.,'No:')]").extract())
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())
        meters = "".join(response.xpath("//tr/td[ b[. ='Metre Kare (Br??t)']]//following-sibling::td/text()").extract())
        room_count = "".join(response.xpath("//tr/td[ b[. ='Oda Say??s??']]//following-sibling::td/text()").extract())
        title = response.xpath("//div[contains(@class,'flex-column')]//h3/text()").extract_first()
        if title:
            item_loader.add_value("title", title)
            if "Sat??l??k" in title or "50450" in external_id:
                return
        item_loader.add_value("square_meters", meters.strip())
        
        item_loader.add_value("external_link", response.url)

        

        price = "".join(response.xpath("//a[@class='price']/text()").extract())
        if price:
            item_loader.add_value("rent_string",price)
    
        if "(" in  room_count:
            item_loader.add_value("room_count", room_count.split("(")[1].split(")")[0])
        elif "+" in  room_count:
            item_loader.add_value("room_count", split_room(room_count, "count"))
        else:
            item_loader.add_value("room_count", room_count)
    
        bathroom_count = "".join(response.xpath("//tr/td[ b[. ='Banyo Say??s??']]//following-sibling::td/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        try:
            floor = "".join(response.xpath("normalize-space(//tr/td[ b[. ='Bulundu??u Kat']]//following-sibling::td/text())").extract())
            item_loader.add_value("floor", floor.replace("+",""))
            item_loader.add_xpath("utilities", "normalize-space(//tr/td[ b[. ='Aidat']]//following-sibling::td/text())")
            item_loader.add_xpath("deposit", "normalize-space(//tr/td[ b[. ='Depozito/Pe??inat']]//following-sibling::td/text())")
        except:
            pass

        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = "".join(response.xpath("//div[@class='card-body']/p/text()").extract())
        item_loader.add_value("description", desc)

        images = [response.urljoin(x)for x in response.xpath("//ol[@class='carousel-indicators']/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)


        address = "".join(response.xpath("//tr/td[b[. ='Konum']]//following-sibling::td/a/text()").extract())
        if address:
            item_loader.add_value("address",re.sub("\s{2,}", " ",address.strip()))
    
        city = "".join(response.xpath("//tr/td[b[. ='Konum']]//following-sibling::td/a[2]/text()").extract())
        if city:
            item_loader.add_value("city",city.strip().replace(",","").strip())

        balcony = "".join(response.xpath("//tr/td[ b[. ='Balkon']]//following-sibling::td/text()").extract())
        if balcony:
            if "var" in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        furnished = "".join(response.xpath("//tr/td[ b[. ='E??yal??']]//following-sibling::td/text()").extract())
        if furnished:
            if "evet" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        elevator = "".join(response.xpath("//div[@class='card-body']/ul/li/text()[contains(.,'Asans??r')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)
        terrace = "".join(response.xpath("//div[@class='card-body']/ul/li/text()[contains(.,'Teras')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        parking = "".join(response.xpath("//div[@class='card-body']/ul/li/text()[contains(.,'Otopark')]").extract())
        if parking:
            item_loader.add_value("parking", True)


        phone = response.xpath("//div[@class='content']/ul/li/a/@href[contains(.,'tel:')]").extract_first()
        if phone:            
            item_loader.add_value("landlord_phone", phone.replace("tel:",""))

        email = response.xpath("//div[@class='content']/a/@href[contains(.,'mailto')]").extract_first()
        if email:
            item_loader.add_value("landlord_email",email.replace('mailto:',"") )

        item_loader.add_xpath("landlord_name", "//div[@class='content']/a[contains(@class,'h5')]/text()")
        
        latlng = response.xpath("//div[@id='map-canvas']//div[1]/a/@href").extract_first()
        if latlng:
            latitude = latlng.split("l=")[1].split(",")[0]
            longitude = latlng.split(",")[1].split("&z")[0]
            item_loader.add_value("latitude",latitude )
            item_loader.add_value("longitude",longitude)

        yield item_loader.load_item()

def split_room(room_count,get):
    count1 = room_count.strip().split("+")[0]
    count2 = room_count.strip().split("+")[1]
    if count2 !="" and count2 is not None: 
        count = int(count1)+int(count2)
        return str(count)
    else:
        count = int(count1.replace("+",""))
        return str(count)