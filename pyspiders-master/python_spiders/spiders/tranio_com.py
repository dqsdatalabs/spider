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
    name = 'tranio_com_disabled'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Tranio_PySpider_italy"
    # LEVEL 1

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://tranio.com/rent/italy/apartments/?order=rank",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://tranio.com/rent/italy/semi-detached/?order=rank",
                    "https://tranio.com/rent/italy/detached/?order=rank", 
                    "https://tranio.com/rent/italy/apartments/penthouse/?order=rank"
                ],
                "property_type": "house"
            },
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
        page=response.meta.get("page",2)
        seen=False
        border=response.xpath("//div[@class='pgs-counter']/text()").get()
        if border:
            border=border.split("of")[-1].strip()

        headers = {
            "authority": "tranio.com",
            "method": "GET",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "upgrade-insecure-requests": "1"
             }
        for item in response.xpath("//div[contains(@class,'snippet-startup')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen=True
        if border:
            if page==2 or page<=int(border):

                if "apartments" in response.url and not "penthouse" in response.url:
                    url="https://tranio.com/rent/italy/apartments/"
                    next_page = f"{url}?page={page}"
                if "apartments" in response.url and "penthouse" in response.url:
                    url="https://tranio.com/rent/italy/apartments/penthouse/"
                    next_page = f"{url}?page={page}"
                if "detached/" in response.url and not "semi" in response.url:
                    url="https://tranio.com/rent/italy/detached/"
                    next_page = f"{url}?page={page}"
                if "semi-detached" in response.url:
                    url="https://tranio.com/rent/italy/semi-detached/"
                    next_page = f"{url}?page={page}"

                if next_page:
                    print(next_page) 
                    yield Request(next_page, callback=self.parse,headers=headers, meta={"property_type": response.meta.get('property_type'),"page":page+1},)

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)


        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("/")[-2])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//div[@class='ap-main-inner']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        else:
            return


        rent=response.xpath("//span[contains(.,'high season')]/text()").get()
        if rent:
            rent=rent.split("€")[0].split("\xa0€")[0].replace("\xa0","").replace(",","")
            rent = int(rent)*4
            item_loader.add_value("rent",rent)
        else:
            rent=response.xpath("//span[contains(.,'transitional')]/text()").get()
            if rent:
                rent=rent.split("€")[0].split("\xa0€")[0].replace("\xa0","").replace(",","")
                rent = int(rent)*4
                item_loader.add_value("rent",rent)
            else:
                rent=response.xpath("//span[contains(.,'low')]/text()").get()
                if rent:
                    rent=rent.split("€")[0].split("\xa0€")[0].replace("\xa0","").replace(",","")
                    rent = int(rent)*4
                    item_loader.add_value("rent",rent)
           
            
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent1=response.xpath("//div[@class='ap-price-main']/span/text()").get()
            if rent1 and "per\xa0week" in rent1:
                rent1=rent1.split("€")[0].strip().replace(",","")
                if rent1:
                    item_loader.add_value("rent",int(rent1)*4)
                rentcheck2=item_loader.get_output_value("rent")
                if not rentcheck2:
                    rent2=response.xpath("//script[contains(.,'price')]/text()").get()
                    if rent2:
                        rent2=rent2.split("price':")[-1].split(",")[0].replace('"',"")
                        if rent2:
                            item_loader.add_value("rent",rent2)
        square_meters=response.xpath("//div[.='Total area']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("\xa0m²")[0])
        room_count=response.xpath("//div[.='Bedrooms']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            room_count = response.xpath("//meta[@property='description']/@content").get()
            if room_count:
                room_count = room_count.split("bedrooms")[0].split(",")[-1]
                item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[.='Bathrooms']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        desc=" ".join(response.xpath("//div[@class='ap-description']/article//p//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        features=response.xpath("//div[.='Features']/following-sibling::ul//li//text()").getall()
        if features:
            for i in features:
                if "balcony" in i.lower():
                    item_loader.add_value("balcony",True)
                if "terrace" in i.lower():
                    item_loader.add_value("terrace",True)
                if "furnished" in i.lower():
                    item_loader.add_value("furnished",True)
        adres=response.xpath("//script[contains(.,'addressCountry')]/text()").get()
        if adres:
            city=adres.split("addressLocality")[-1].split(",")[0].replace(":","").replace('"',"")
            if city:
                country=adres.split("addressCountry")[-1].split("name")[-1].split("}")[0].replace(":","").replace('"',"")
                item_loader.add_value("city",city)
                item_loader.add_value("address",city+" "+country)
        images=[response.urljoin(x) for x in response.xpath("//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        
        item_loader.add_value("currency","EUR")
        name=response.xpath("//div[@class='lf-manager-info']/div/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        item_loader.add_value("landlord_phone","+44 17 4822 0039")
        item_loader.add_value("landlord_email","info@tranio.com")

        yield item_loader.load_item()