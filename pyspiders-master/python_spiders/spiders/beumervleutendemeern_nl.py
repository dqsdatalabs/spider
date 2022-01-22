# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'beumervleutendemeern_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    headers = {
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'cache-control': "no-cache",
    }
    def start_requests(self):
        payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"__live\"\r\n\r\n1\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"__infinite\"\r\n\r\nitem\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumermaarssen.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumerutrecht.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumervleutendemeern.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumerwijkbijduurstede.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"koophuur\"\r\n\r\nhuur\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"plaats_postcode\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"radiuscustom\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"typewoning\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"prijshuur[min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"prijshuur[max]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"status[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"woningsoort[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"liggingen[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"buitenruimtes[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"bouwperiode[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"energielabel[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"voorzieningen[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"openHuis[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"nieuwAanbod[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"woonOppervlakte\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"perceelOppervlakte\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"aantalKamers\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"slaapkamers\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"subscribe_email\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"orderby\"\r\n\r\ncustom_order:asc,publicatiedatum:desc\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
        
        yield Request(url="https://beumervleutendemeern.nl/woningen/page/1/",
                                 callback=self.parse,
                                 method="POST",
                                 body=payload,
                                 headers=self.headers)


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        data = json.loads(response.body)
        for item in data["items"]:
            sel = Selector(text=item, type="html")
            f_url = sel.xpath("//div[@class='card__outer']/a/@href").get()
            prop_type = sel.xpath("//div[@class='card__inner']/footer/div[contains(@class,'card__type')]/a/text()").get()
            if "Appartement" in prop_type:
                prop_type = "apartment"
            elif "Woonhuis" in prop_type:
                prop_type = "house"
            else:
                prop_type = None
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : prop_type},
            )
            seen = True

        if page == 2 or seen:
            url = f"https://beumervleutendemeern.nl/woningen/page/{page}/"
            payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"__live\"\r\n\r\n1\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"__infinite\"\r\n\r\nitem\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumermaarssen.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumerutrecht.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumervleutendemeern.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"makelaar[]\"\r\n\r\nbeumerwijkbijduurstede.nl\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"koophuur\"\r\n\r\nhuur\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"plaats_postcode\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"radiuscustom\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"typewoning\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"prijshuur[min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"prijshuur[max]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"status[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"woningsoort[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"liggingen[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"buitenruimtes[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"bouwperiode[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"energielabel[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"voorzieningen[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"openHuis[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"nieuwAanbod[]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"woonOppervlakte\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"perceelOppervlakte\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"aantalKamers\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"slaapkamers\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"subscribe_email\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"orderby\"\r\n\r\ncustom_order:asc,publicatiedatum:desc\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
            yield Request(url=url,
                        callback=self.parse,
                        method="POST",
                        body=payload,
                        headers=self.headers,
                        meta={"page":page+1})
        

          
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Beumervleutendemeern_nl_PySpider_" + self.country + "_" + self.locale)
        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("room_count", "//tr[th[.='Aantal kamers']]/td/text()")
        item_loader.add_xpath("floor", "//tr[th[.='Aantal verdiepingen']]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Aantal badkamers']]/td/text()")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        rent =  response.xpath("//tr[th[.='Huurprijs']]/td/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent)
        else:
            rent = response.xpath("//th[contains(text(),'prijs')]//following-sibling::td/text()").get()
            if rent:
                item_loader.add_value("rent_string", rent.split("k.k")[1])

        desc = "".join(response.xpath("//div[@class='small8 columns bd-m60b sd-m40b']/div/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        meters = "".join(response.xpath("//tr[th[.='Woonoppervlakte']]/td/text()").extract())
        if meters:
            s_meters = meters.split("m²")[0].strip()
            item_loader.add_value("square_meters",s_meters)

        floor_plan_images = [x for x in response.xpath("//a[contains(@class,'wonen__mask js--trigger__viewer_floorplan')]/following-sibling::img/@data-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        images = [x for x in response.xpath("//div[@class='wonen__slider-item']/img//@data-src[not(contains(.,'163x118'))]").getall()]
        if images:
            item_loader.add_value("images", images)


        address = "".join(response.xpath("//div[@class='content']//article/section//div[@class='row bd-m60t sd-m40y']/div//h2/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())

        zipcode = "".join(response.xpath("//h1/text()[2]").extract())
        if zipcode:
            zipcod=' '.join(zipcode.strip().split(" ")[:2])
            item_loader.add_value("zipcode", zipcod)
        
        city = response.xpath("//h1/text()[last()]").get()
        if city: item_loader.add_value("city", city.strip().split(' ')[-1].strip())

        latlong = response.xpath("//iframe/@data-src-cmplz").extract_first()
        if latlong:
            lat  = latlong.split("lat=")[1].split("&")[0]
            lng  = latlong.split("lon=")[1].split("&")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude",lng)            

        item_loader.add_value("landlord_phone", "030-6776000")
        item_loader.add_value("landlord_name", "Beumervleutendemeern")
        item_loader.add_value("landlord_email", "info@beumer.nl")

        if not response.xpath("//div[@class='wonen__tag']/p/text()[contains(.,'Verhuurd')]").get():
            yield item_loader.load_item()
