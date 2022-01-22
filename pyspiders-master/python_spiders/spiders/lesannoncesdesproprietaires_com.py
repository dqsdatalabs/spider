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
    name = 'lesannoncesdesproprietaires_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source="Lesannoncesdesproprietaires_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "type" : "M",
                "property_type" : "house"
            },
            {
                "type" : "A",
                "property_type" : "apartment"
            },
        ]
        headers = {
            'X-CSRF-TOKEN': 'idbx1s2Swiyi7SX6xAlyPCWAxTFOC8NPRbM0hnNX',
            'X-Requested-With': 'XMLHttpRequest',
            'X-XSRF-TOKEN': 'eyJpdiI6Ik5nQWpSMWdaSkV5cWdmV1lXZFNYMFE9PSIsInZhbHVlIjoiMGYxemRYZjNrbDR2dVNyeEZlVjFqZDd1c01NS1FlajFKK1VPdUtxaHAySWJBaVwveUNveFU3WldpZlJtU09Yd2lKRTdLWXppckxhUndCSFBEVTVpTUdnPT0iLCJtYWMiOiJlM2FjMTZlZTYzYThiNGRkMmRlZTZlYzIyNTNlZDhjOWIxY2UzNmFhMzgyOWY0ODJmNTk2MjBkZGNiMmM5M2QyIn0=',
            'Cookie': '__cfduid=d281a0c584bff69c0e02e210070159a291607952702; JSESSIONID=854214EB578233273AD99E6F8AC3EB54.cfusion; CFID=9759934; CFTOKEN=53140436'

        }
        for url in start_urls:

            formdata = {
                "sort": "Offre_classementLADP2014,Offre_dateclassementLADP2014 desc,offre_nphotos desc",
                "choixVL": "L",
                "Offre_secteur": "",
                "distance": "0",
                "Offre_type": url.get("type"),
                "Offre_loyerHC": "",
            }

            yield FormRequest(
                url="https://lesannoncesdesproprietaires.com/resultat.cfm",
                callback=self.parse,
                formdata=formdata,
                headers=headers,
                meta={'property_type': url.get('property_type')}
            )
        

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@href,'locations')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)

        property_type = response.meta.get('property_type')
        type_studio = response.xpath("//div[@class='caracteristiques']/div[@class='carac pull-right']/text()[contains(.,'Studio')]").get()
        if type_studio:
            property_type = "studio"
        item_loader.add_value("property_type", property_type)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = "".join(response.xpath("//div[@class='row fiche']//h2/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip())

        address = "".join(response.xpath("//div[@class='row fiche']//div[@class='ville']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        external_id = "".join(response.xpath("//div[@class='row fiche']//div[@class='legende']/div[1]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        available_date=response.xpath("//div[@class='row fiche']//div[@class='legende']/div[2]/text()").get()

        if available_date:
            date2 =  available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//div[@class='row fiche']//div[@class='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        if "vendre" in description.lower(): return
        
        room_count = " ".join(response.xpath("//div[@class='row fiche']//div[@class='carac pull-right']/text()").getall()).strip()   
        if room_count:
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            else:
                item_loader.add_value("room_count", room_count.replace('T', '').replace("bis","").strip())

        images = [ x.split("?")[0] for x in response.xpath("//div/div[contains(@class,'item')]/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latlng = "".join(response.xpath("//script[contains(.,'LatLng')]/text()").extract())
        if latlng:

            item_loader.add_xpath("latitude", "substring-before(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),',')")
            item_loader.add_xpath("longitude", "substring-before(substring-after(//script[contains(.,'LatLng')]/text(),','),')')")

        item_loader.add_value("landlord_name", "Annoncesdes Proprietaires")
        item_loader.add_value("landlord_phone", "05.56.04.04.04")
        item_loader.add_value("landlord_email","dl.bordeaux-bastide@directe-location.com")
        
        yield item_loader.load_item()
