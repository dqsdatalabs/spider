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
class MySpider(Spider):
    name = 'sharpesestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    def start_requests(self):
        formdata = {
            "how": "byPage",
            "perPage": "100",
            "filter[id1422273614114][addrSearch]": "",
            "filter[id1422289669592][min]": "",
            "filter[id1422289669592][max]": "",
            "priceFilter[min]": "",
            "priceFilter[max]": "",
        }
        url = "https://wix.shareiiit.com/realEstate/info.php?instance=UKNvZdtNBqD7rb9G0RcPdaIQ9UD8fBQnUcNYnGHmlsE.eyJpbnN0YW5jZUlkIjoiMjFhMDRmZTktMDA0Ni00ODUwLTk1NTAtOGQwYWRhMmY5ZWY1IiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDEtMTNUMDg6MDg6NTYuMTY3WiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6ImM1MjFkODA4LWRlOTAtNGJlYy05ODU4LWQ1ZDE1NDg2MTUyMyIsInNpdGVPd25lcklkIjoiNWFmY2ZhNDMtMWU3YS00ZTQ0LTlmN2YtMmY4NzM2YmQzMWNhIn0&pageId=mwxwu&compId=TPASection_j2t735za&viewerCompId=TPASection_j2t735za&siteRevision=403&viewMode=site&deviceType=desktop&locale=en&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%22b5c1748e-0504-4078-abbc-7e9d50bf06c4%7C2%22%2C%22BSI%22%3A%22b5c1748e-0504-4078-abbc-7e9d50bf06c4%7C2%22%7D&vsi=28a650ab-91bc-4487-bf21-d3cb31fc5e7e&width=1157&height=3097&section-url=https%3A%2F%2Fwww.sharpesestates.com%2Frentals%2F&target=_top"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False

        data = json.loads(response.body)
        for item in data["assets"]:
            follow_url = f"https://www.sharpesestates.com/rentals/propE{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"_data":item})
        
        if page == 1 or seen:
            formdata = {
                "how": "byPage",
                "perPage": "100",
                "filter[id1422273614114][addrSearch]": "",
                "filter[id1422289669592][min]": "",
                "filter[id1422289669592][max]": "",
                "priceFilter[min]": "",
                "priceFilter[max]": "",
                "offset": str(page),
            }
            url = "https://wix.shareiiit.com/realEstate/info.php?instance=UKNvZdtNBqD7rb9G0RcPdaIQ9UD8fBQnUcNYnGHmlsE.eyJpbnN0YW5jZUlkIjoiMjFhMDRmZTktMDA0Ni00ODUwLTk1NTAtOGQwYWRhMmY5ZWY1IiwiYXBwRGVmSWQiOiIxM2FhMmZkMi0xZDc0LTE0NjUtNDYzNC01YTM5NGQwNzBhZWUiLCJzaWduRGF0ZSI6IjIwMjEtMDEtMTNUMDg6MDg6NTYuMTY3WiIsInZlbmRvclByb2R1Y3RJZCI6InByZW1pdW0iLCJkZW1vTW9kZSI6ZmFsc2UsImFpZCI6ImM1MjFkODA4LWRlOTAtNGJlYy05ODU4LWQ1ZDE1NDg2MTUyMyIsInNpdGVPd25lcklkIjoiNWFmY2ZhNDMtMWU3YS00ZTQ0LTlmN2YtMmY4NzM2YmQzMWNhIn0&pageId=mwxwu&compId=TPASection_j2t735za&viewerCompId=TPASection_j2t735za&siteRevision=403&viewMode=site&deviceType=desktop&locale=en&commonConfig=%7B%22brand%22%3A%22wix%22%2C%22bsi%22%3A%22b5c1748e-0504-4078-abbc-7e9d50bf06c4%7C2%22%2C%22BSI%22%3A%22b5c1748e-0504-4078-abbc-7e9d50bf06c4%7C2%22%7D&vsi=28a650ab-91bc-4487-bf21-d3cb31fc5e7e&width=1157&height=3097&section-url=https%3A%2F%2Fwww.sharpesestates.com%2Frentals%2F&target=_top"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Sharpesestates_PySpider_united_kingdom")      

        data = response.meta["_data"]
        desc = data["data"]["id1422289759907"]["val"]["val"]
        if desc:
            item_loader.add_value("description", desc)
        else:
            desc = ""
  
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        address = data["data"]["id1422273614114"]["val"]["val"]
        room_count = data["data"]["id1422289669592"]["val"]["val"]
        bathroom_count = data["data"]["id1422289688432"]["val"]["val"]
        external_id = data["data"]["id1422289483499"]["val"]["val"]
        available_date = data["data"]["id1515496276741"]["val"]["val"]
        title = data["title"]
        rent = data["price"]
        item_loader.add_value("title",title)
        if not address and title:
            address = title 
        if address:
            address = address.replace("&#44;",",")
            item_loader.add_value("address",address)
        if title:
            city = ""
            zipcode = ""
            if len(title.split(","))>2:
                city = title.split(",")[-2].strip()
                zipcode = title.split(",")[-1].strip()
            else:            
                city_zipcode = address.split(",")[-1].strip()
                if address.split(",")[-1].strip()=="UK":
                    city_zipcode = address.split(",")[-2].strip()
                zipcode = city_zipcode.split(" ")[-2]+" "+city_zipcode.split(" ")[-1]
                city = city_zipcode.replace(zipcode,"")
            if city:
                item_loader.add_value("city",city)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        if room_count:
            if "/" in room_count:
                room_count = room_count.split("/")[0]
        item_loader.add_value("room_count",room_count)
        item_loader.add_value("bathroom_count",bathroom_count)
        item_loader.add_value("external_id",external_id)
        item_loader.add_value("rent_string",rent)
        lat = data["data"]["id1422290161888"]["val"]["centerLat"]
        lng = data["data"]["id1422290161888"]["val"]["centerLng"]
        if lat and lng and lat !="10":
            item_loader.add_value("latitude", str(lat))
            item_loader.add_value("longitude", str(lng))

        images = [x["uri"] for x in data["images"]]
        if images:
            item_loader.add_value("images", images)
        furnished = data["data"]["id1495019481553"]["val"]["checked"]
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)
        parking = data["data"]["id1504628261822"]["val"]["checked"]
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)
        if available_date:  
            date_parsed = dateparser.parse(available_date.replace("Available","").strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        item_loader.add_value("landlord_name", "SHARPES ESTATES")
        item_loader.add_value("landlord_phone", "208 286 4073")
        item_loader.add_value("landlord_email", "info@sharpesestates.com")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None