# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'servicedapartmentsindublin_ie'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Servicedapartmentsindublin_PySpider_ireland"

    headers={
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cookie": "_gcl_au=1.1.183359815.1637831079; bSession=4fac6b6f-7d4b-4f89-92a5-4e5ff03be217|6",
        "referer": "https://hotels.wixapps.net/index.html?pageId=crop&compId=i6klgqap_0&viewerCompId=i6klgqap_0&siteRevision=198&viewMode=site&deviceType=desktop&locale=en&regionalLanguage=en&width=980&height=7224&instance=m5laRyJ6kB-krAVfCsD37wDzjLaOq0ywv9UuoYhC64M.eyJpbnN0YW5jZUlkIjoiOGY5NjUxZDgtYjY3Yy00MDVlLWI4MjMtYjYxZTlkOGUyN2FlIiwiYXBwRGVmSWQiOiIxMzVhYWQ4Ni05MTI1LTYwNzQtNzM0Ni0yOWRjNmEzYzliY2YiLCJtZXRhU2l0ZUlkIjoiYjA2Mzg0NDYtYTM0OC00OTk3LWE4MDYtZmQzMDIzYjA3ZjAwIiwic2lnbkRhdGUiOiIyMDIxLTExLTI1VDA5OjAxOjA5LjU4M1oiLCJ2ZW5kb3JQcm9kdWN0SWQiOiJVbmxpbWl0ZWQiLCJkZW1vTW9kZSI6ZmFsc2UsIm9yaWdpbkluc3RhbmNlSWQiOiIxM2UyNTliZi03ZGZmLTkzZGYtNzkzZS1mOWE5N2E0Yjg5ZWIiLCJhaWQiOiJiNzI4OGNkMy01YTcxLTQwYmUtYTU1NS04ZjA5YzYwZjgxYzMiLCJiaVRva2VuIjoiM2ZmNWQ1OWUtMTUzNC0wOWM5LTEwMjUtNGIyZWJlM2U1OGFlIiwic2l0ZU93bmVySWQiOiI0NjcyMWMyOC1kOTY5LTQxNGMtYTYyZS0zYzNjZTA4YTczOWYifQ&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%224fac6b6f-7d4b-4f89-92a5-4e5ff03be217%7C6%22%2C%22BSI%22%3A%224fac6b6f-7d4b-4f89-92a5-4e5ff03be217%7C6%22%7D&target=_top&section-url=https%3A%2F%2Fwww.servicedapartmentsindublin.ie%2Fapartments%2F&vsi=09374898-34a9-4150-8dcc-12f6a80de890",
        "x-wix-instance": "m5laRyJ6kB-krAVfCsD37wDzjLaOq0ywv9UuoYhC64M.eyJpbnN0YW5jZUlkIjoiOGY5NjUxZDgtYjY3Yy00MDVlLWI4MjMtYjYxZTlkOGUyN2FlIiwiYXBwRGVmSWQiOiIxMzVhYWQ4Ni05MTI1LTYwNzQtNzM0Ni0yOWRjNmEzYzliY2YiLCJtZXRhU2l0ZUlkIjoiYjA2Mzg0NDYtYTM0OC00OTk3LWE4MDYtZmQzMDIzYjA3ZjAwIiwic2lnbkRhdGUiOiIyMDIxLTExLTI1VDA5OjAxOjA5LjU4M1oiLCJ2ZW5kb3JQcm9kdWN0SWQiOiJVbmxpbWl0ZWQiLCJkZW1vTW9kZSI6ZmFsc2UsIm9yaWdpbkluc3RhbmNlSWQiOiIxM2UyNTliZi03ZGZmLTkzZGYtNzkzZS1mOWE5N2E0Yjg5ZWIiLCJhaWQiOiJiNzI4OGNkMy01YTcxLTQwYmUtYTU1NS04ZjA5YzYwZjgxYzMiLCJiaVRva2VuIjoiM2ZmNWQ1OWUtMTUzNC0wOWM5LTEwMjUtNGIyZWJlM2U1OGFlIiwic2l0ZU93bmVySWQiOiI0NjcyMWMyOC1kOTY5LTQxNGMtYTYyZS0zYzNjZTA4YTczOWYifQ"
    }

    def start_requests(self):
        url = "https://hotels.wixapps.net/api/rooms?timestamp=1637831308002"
        yield Request(url, callback=self.parse,headers=self.headers)   

    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)
        for url in data:
            follow_url=url["roomId"]
            print("folloe",url)
            follow_url1=f"https://www.servicedapartmentsindublin.ie/apartments/rooms/"+follow_url
            yield Request(follow_url1, callback=self.populate_item,meta={"item":url})
            

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item=response.meta.get("item")

        title=item["name"]
        if title: item_loader.add_value("title",title)
        room_count=item["beds"]
        for i in room_count:
            if "double" in str(i):
                double=str(i).split("count':")[-1].replace("}","").strip()
                if double!="0":
                    item_loader.add_value("room_count",double)
            if "queen" in str(i):
                queen=str(i).split("count':")[-1].replace("}","").strip()
                if queen!="0":
                    item_loader.add_value("room_count",queen)
        description=item["longDesc"]
        if description:
            item_loader.add_value("description",description)
        square_meters=item["size"]
        if square_meters:
            item_loader.add_value("square_meters",str(square_meters).split(".")[0])
        rent=item["price"]["amount"]
        if rent:
            item_loader.add_value("rent",str(rent).split(".")[0])
        item_loader.add_value("currency","EUR")
        features=str(item["amenities"])
        if features:
            if "washing machine" in features:
                item_loader.add_value("washing_machine",True)
        images=[f"https://static.wixstatic.com/media/"+x for x in item["images"]]
        if images:
            item_loader.add_value("images",images)
        adres=str(item["address"]["formatted"])
        if adres:
            item_loader.add_value("address",adres)
        item_loader.add_value("city","Dublin")
        item_loader.add_value("property_type","apartment")
        lat=str(item["address"]["unformatted"])
        if lat:
            item_loader.add_value("latitude",lat.split("geometry")[1].split("lat")[1].split(",")[0].replace('":',""))
        lng=str(item["address"]["unformatted"])
        if lng:
            item_loader.add_value("longitude",lng.split("geometry")[1].split("lng")[1].split(",")[0].replace('":',""))
            
        yield item_loader.load_item()