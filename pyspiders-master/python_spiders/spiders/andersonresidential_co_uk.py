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
    name = 'andersonresidential_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Andersonresidential_Co_PySpider_united_kingdom"
    custom_settings = {
        # "PROXY_ON":"True",
        "HTTPCACHE_ENABLED":False,
    }
    headers={
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie": "consent=1,2,3,4,5,6; consent=1,2,3,4,5,6; _ga=GA1.3.877101613.1637926147; _gid=GA1.3.2005735866.1637926147; giosg_gid_3656=rkckjcj6liramw2xt4aafyvyokcnrhir5ok56ascvqiqacqm; giosg_chat_id_3656=x53y5iv7q4s23p2gaeaaoee2twcfzgc66vg3ikljjzfb7nym; giosg_gsessid_3656=14ebe702-4eac-11ec-a25f-0242ac120005; cookieconsent_status=allow; _gat_UA-88158359-31=1; _ctesius2_session=WmhlV25uZnhZU1M0SktGR1pRQWxLM2pQcElTcE5TMnJwYVhETXZuTmJpMnNaT1V1WWx0T01SbDBIOTZ0bTNBQnBWd3EwUHNiYTB4MUFkVzVBbG9tYjZQVjNON05qRS96WS9kRkN2azc1azlSL1JtaFh6ZEJ3UnFlZ3VBRWdIQS9oVXc5WjZIeWI2M0pCVWlUNGZ5alFnPT0tLXJxdlc0NWNIUllTZE9CUXN4M1llQ2c9PQ%3D%3D--834f76975373978765cee92d4ae639df0bf10305",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    }
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.andersonresidential.co.uk/search.ljson?channel=lettings&fragment=tag-flat/most-recent-first/status-all/page-1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.andersonresidential.co.uk/search.ljson?channel=lettings&fragment=tag-house/most-recent-first/status-all/page-1"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,headers=self.headers,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        data=json.loads(response.body)['properties']
        for item in data:
            bathrooms=item['bathrooms']
            item=f"https://www.andersonresidential.co.uk/"+item['url']
            yield Request(item, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"bathrooms":bathrooms})
            seen = True
        if page == 2 or seen:
            a=f"page-{page}"
            url = str(response.url).replace("page-1",a)
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item=response.meta.get('item')
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        address ="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if address:
            item_loader.add_value("address", address.split("-property-address:")[1].split("-->")[0].replace('"',""))
        zipcode ="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("property-postcode:")[1].split("-->")[0].replace('"',""))
        rent = "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if rent:
            item_loader.add_value("rent", rent.split("property-price:")[1].split("-->")[0].replace('"',""))
        item_loader.add_value("currency", "GBP")
        
        room_count = "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        item_loader.add_value("room_count", room_count.split('property-bedrooms:"')[1].split("-->")[0].replace('"',""))
        
        bathroom_count =response.meta.get('bathrooms')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        description = "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if description:
            item_loader.add_value("description",description.split('--property-description:"')[1].split("-->")[0].replace('"',""))
            
        images= "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if images:
            images = [x.split('"-->')[0]for x in images.split('--property-images:"')]
            if images:
                item_loader.add_value("images", images)
                
        latitude=  "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if latitude:
            item_loader.add_value("latitude", latitude.split('property-latitude:"')[1].split('"-->')[0])   
                       
        longitude=  "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if longitude:
            item_loader.add_value("longitude", longitude.split('--property-longitude:"')[1].split('"-->')[0])     
        
        available_date="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if available_date:
            item_loader.add_value("available_date",available_date.split("property-live-date:")[1].split("-->")[0].replace('"',""))
        
        name="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if name:
            item_loader.add_value("landlord_name",name.split('--property-office-name:"')[1].split("-->")[0].replace('"',""))
                
        email="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if email:
            item_loader.add_value("landlord_email",email.split('--property-email:"')[1].split("-->")[0].replace('"',""))
        
        yield item_loader.load_item()