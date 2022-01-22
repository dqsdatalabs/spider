# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser 
import re

class MySpider(Spider):
    name = 'wex_commercial'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Wex_Commercial_PySpider_united_kingdom"

    def start_requests(self):


        formdata ={
            'how': 'byPage',
            'perPage': '10',
            'filter[id1422289550652]': 'formBuilder-dropdownOption89418',
            'filter[id1422289622959]': 'formBuilder-dropdownOption72631',
            'filter[id1422273614114][addrSearch]': '',
            'filter[id1422289669592][min]': '',
            'filter[id1422289669592][max]': '',
            'priceFilter[min]': '',
            'priceFilter[max]': '',
            'offset': '1'
            }
            

        yield FormRequest("https://wix.shareiiit.com/realEstate/info.php?lang=en&dateNumberFormat=en-us&isPrimaryLanguage=true&pageId=fnso0&compId=TPASection_jwj59zyq&viewerCompId=TPASection_jwj59zyq&siteRevision=1583&viewMode=site&deviceType=desktop&locale=en&tz=Europe%2FLondon&regionalLanguage=en&width=1356&height=3039&instance=QKXZus4Z_VuHhjmw3e3_3q7m8MinIxyit45urwvMITU.eyJpbnN0YW5jZUlkIjoiOTliNjJlZDMtNTRjMy00YjkwLWJjYjItNTVhY2UwMDZmM2UyIiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDctMTNUMTE6MTg6MzEuNzAxWiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6Ijk4ZmZjOGVhLTdkMDItNGE1NS04ZWIwLWYwOGQ1ZmJlODEzYSIsInNpdGVPd25lcklkIjoiYWQwZGMwMGItMWEzNy00NTA4LThiOGItNDE1OWYwZDA5OTY2In0&currency=GBP&currentCurrency=GBP&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%22c57edf8c-5c35-4bb3-b329-2f3ab1424a6b%7C5%22%2C%22BSI%22%3A%22c57edf8c-5c35-4bb3-b329-2f3ab1424a6b%7C5%22%7D&target=_top&section-url=https%3A%2F%2Fwww.wexcommercial.com%2Fproperties%2F&vsi=a233b6e6-d0a5-46fc-a436-6bfd3f6b761f",
                    callback=self.parse, 
                    formdata=formdata,
                    )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        jseb = json.loads(response.body)
        # print(jseb)
        for j in jseb["assets"]:
            
            images = j["images"]
            detail_id = j["id"]
            price = j["price"].replace(",","").split("/")[0].strip()
            title = j["title"]
            detail_url = f"https://www.wexcommercial.com/properties/propE{detail_id}"
            room = j["data"]["id1422289669592"]["val"]["val"]
            bath = j["data"]["id1422289688432"]["val"]["val"]
            description=j["data"]["id1422289759907"]["val"]["val"]
            img = []
            for  i in images:
                image = i["uri"]
                img.append(image)
            yield Request(detail_url, callback=self.populate_item,meta={"title":title,"price":price,"img":img,"room":room,"bath":bath,"description":description,"detail_id":detail_id})
            seen = True
        if page == 2 or page <= int(jseb["perPage"]):
            formdata = {
                'how': 'byPage',
                'perPage': '10',
                'filter[id1422289550652]': 'formBuilder-dropdownOption89418',
                'filter[id1422289622959]': 'formBuilder-dropdownOption72631',
                'filter[id1422273614114][addrSearch]': '',
                'filter[id1422289669592][min]': '',
                'filter[id1422289669592][max]': '',
                'priceFilter[min]': '',
                'priceFilter[max]': '',
                'offset': f'{page}',
            }
            yield FormRequest("https://wix.shareiiit.com/realEstate/info.php?lang=en&dateNumberFormat=en-us&isPrimaryLanguage=true&pageId=fnso0&compId=TPASection_jwj59zyq&viewerCompId=TPASection_jwj59zyq&siteRevision=1583&viewMode=site&deviceType=desktop&locale=en&tz=Europe%2FLondon&regionalLanguage=en&width=1356&height=3039&instance=QKXZus4Z_VuHhjmw3e3_3q7m8MinIxyit45urwvMITU.eyJpbnN0YW5jZUlkIjoiOTliNjJlZDMtNTRjMy00YjkwLWJjYjItNTVhY2UwMDZmM2UyIiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDctMTNUMTE6MTg6MzEuNzAxWiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6Ijk4ZmZjOGVhLTdkMDItNGE1NS04ZWIwLWYwOGQ1ZmJlODEzYSIsInNpdGVPd25lcklkIjoiYWQwZGMwMGItMWEzNy00NTA4LThiOGItNDE1OWYwZDA5OTY2In0&currency=GBP&currentCurrency=GBP&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%22c57edf8c-5c35-4bb3-b329-2f3ab1424a6b%7C5%22%2C%22BSI%22%3A%22c57edf8c-5c35-4bb3-b329-2f3ab1424a6b%7C5%22%7D&target=_top&section-url=https%3A%2F%2Fwww.wexcommercial.com%2Fproperties%2F&vsi=a233b6e6-d0a5-46fc-a436-6bfd3f6b761f", 
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={"page":page+1}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.meta.get("detail_id"))

        item_loader.add_value("external_source", "Wex_Commercial_PySpider_united_kingdom")      
        item_loader.add_value("property_type", "apartment")      
        item_loader.add_value("room_count", response.meta.get("room"))
        item_loader.add_value("bathroom_count", response.meta.get("bath"))
        item_loader.add_value("title", response.meta.get("title"))
        item_loader.add_value("description",response.meta.get("description"))
        item_loader.add_value("address", response.meta.get("title"))
        item_loader.add_value("zipcode", response.meta.get("title").split(",")[-1].strip())
        item_loader.add_value("city", response.meta.get("title").split(",")[:-2])
        item_loader.add_value("images", response.meta.get("img"))

        rent =  response.meta.get("price").replace("P","").strip()
        if rent:
            item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name","WEX&CO")
        item_loader.add_value("landlord_phone","020 8904 0747")
        item_loader.add_value("landlord_email","info@wexandco.com")

     
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
