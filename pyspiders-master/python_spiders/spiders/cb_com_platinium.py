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
    name = 'cb_com_platinium'
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=65&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=9&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=3&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=68&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=2&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=4&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                    "https://www.cb.com.tr/tr-Tr/Stocks/Search?officeid=86&sorting=createdate%2c2&stocknameornumber=&mq=&stockprocesstype=0&scid=1&cscid=5&minprice=&maxprice=&currency=&roomCount=&countryid=&cityid=0&selectedcounties=&selecteddistricts=&officeuserid=&pager_p=1",
                ],
                "property_type" : "house"
            },
            

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='flex-grow-1']"):
            title=item.xpath("./a/text()").get()
            if "satılık" not in title.lower():
                follow_url = response.urljoin(item.xpath(".//a[@class='title h5']/@href").get())
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
        item_loader.add_value("external_source", "Cbcomtrplatinium_PySpider_" + self.country + "_" + self.locale)
        
        title=response.xpath("//div[contains(@class,'flex-column')]//h3/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        external_id = "".join(response.xpath("//div[@class='feature-item']/text()[contains(.,'No:')]").extract())
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())

        price = "".join(response.xpath("//a[@class='price']/text()").extract())
        if price:
            item_loader.add_value("rent",price.split("₺")[0].strip())
            item_loader.add_value("currency", "TRY")
        
        desc = "".join(response.xpath("//div[@id='detail-expand']/p/text()").extract())
        desc = desc.replace('\n', '')
        if desc:
            item_loader.add_value("description", desc)
        
        if "teras" in desc.lower():
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))

        meters = "".join(response.xpath("//tr/td[ b[. ='Metre Kare (Brüt)']]//following-sibling::td/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.strip())
        elif "m2" in desc:
            sqm=desc.split("m2")[0].strip().split(" ")[-1]
            if sqm.isdigit():
                item_loader.add_value("square_meters", sqm)


        room_count = "".join(response.xpath("//tr/td[ b[. ='Oda Sayısı']]//following-sibling::td/text()").extract())
        if "(" in  room_count:
            item_loader.add_value("room_count", room_count.split("(")[1].split(")")[0])
        elif "+" in  room_count:
            item_loader.add_value("room_count", split_room(room_count, "count"))
        elif room_count:
            item_loader.add_value("room_count", room_count)
        elif "+" in title:
            count=title.count("+")
            item_loader.add_value("room_count",split_room2(title,"count2", count))
        elif "+" in desc:
            count=desc.count("+")
            item_loader.add_value("room_count",split_room2(desc,"count2", count))
            
        try:
            floor = "".join(response.xpath("normalize-space(//tr/td[ b[. ='Bulunduğu Kat']]//following-sibling::td/text())").extract())
            if floor.isdigit():
                item_loader.add_value("floor", floor.replace("+",""))
            item_loader.add_xpath("utilities", "normalize-space(//tr/td[ b[. ='Aidat']]//following-sibling::td/text())")
            item_loader.add_xpath("deposit", "normalize-space(//tr/td[ b[. ='Depozito/Peşinat']]//following-sibling::td/text())")
        except:
            pass
        
        bathroom_count=response.xpath("//tr/td[ b[. ='Banyo Sayısı']]//following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        latitude_longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude=latitude_longitude.split("lat = '")[1].split("'")[0]
            longitude=latitude_longitude.split("lng = '")[1].split("'")[0]
            if latitude and longitude:
                item_loader.add_value("latitude" , latitude)
                item_loader.add_value("longitude" , longitude)
        
        desc = "".join(response.xpath("//div[@class='card-body']/p/text()").extract())
        desc = desc.replace('\n', '')
        item_loader.add_value("description", desc)

        images = [response.urljoin(x)for x in response.xpath("//ol[@class='carousel-indicators']/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)


        address = "".join(response.xpath("//tr/td[b[. ='Konum']]//following-sibling::td/a/text()").extract())
        if address:
            item_loader.add_value("address",re.sub("\s{2,}", " ",address.strip()))
        
        lat_lng = response.xpath("//script[contains(.,'lat')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("lat = '")[1].split("'")[0]
            lng = lat_lng.split("lng = '")[1].split("'")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
    
        city = "".join(response.xpath("//tr/td[b[. ='Konum']]//following-sibling::td/a[2]/text()").extract())
        if city:
            item_loader.add_value("city",city.strip().replace(",",""))

        balcony = "".join(response.xpath("//tr/td[ b[. ='Balkon']]//following-sibling::td/text()[contains(.,'Var')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        furnished = "".join(response.xpath("//tr/td[ b[. ='Eşyalı']]//following-sibling::td/text()").extract())
        if furnished:
            if "Hayır" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        elevator = "".join(response.xpath("//div[@class='card-body']/ul/li/text()[contains(.,'Asansör')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)


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

def split_room2(room_count,get,count):
    if count==1:
        room1=room_count.split("+")[0].split(" ")[-1]
        room2=room_count.split("+")[1].split(" ")[0].replace(",","")
        count2=int(room1)+int(room2)
        return str(count2)
    elif count==2:
        room1=room_count.split("+")[1].split(" ")[-1]
        room2=room_count.split("+")[2].split(" ")[0].replace(",","")
        count2=int(room1)+int(room2)
        return str(count2)

        
        
          

        

      
     