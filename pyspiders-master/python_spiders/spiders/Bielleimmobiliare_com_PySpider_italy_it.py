import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import math
import requests

class bielleimmobiliare_PySpider_italySpider(scrapy.Spider):
    name = "bielleimmobiliare_com"
    allowed_domains = ["bielleimmobiliare.com"]
    page_number = 2
    start_urls = [
        "https://www.bielleimmobiliare.com/web/immobili.asp?cod_categoria=R&tipo_contratto=A&language=ita&pagref=71653&num_page=1"
    ]
    country = "italy"
    locale = "it"
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = "testing"

    def parse(self, response):
        urls = response.css(
            "#content > div > div > div.span9.listing > div.row > div > a::attr(href)"
        ).extract()
        for url in urls:
            url = "https://www.bielleimmobiliare.com" + url
            yield Request(url=url, callback=self.parse_property)
        next_page = ("https://www.bielleimmobiliare.com/web/immobili.asp?cod_categoria=R&tipo_contratto=A&language=ita&pagref=71653&num_page="+str(bielleimmobiliare_PySpider_italySpider.page_number))
        if bielleimmobiliare_PySpider_italySpider.page_number <= 4:
            bielleimmobiliare_PySpider_italySpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        external_id = (
            response.css(
                "#maincol > div.row.single-property > div.span3.maininfo > span.for-sale.sfondo_colore1.colore3::text"
            )
            .get()
            .split("Rif: ")[1]
        )
        title = response.css("#subheader > div > div > div.span8 > h1::text").get()
        description = response.css(
            "#maincol > div.lt_tab > div > div.lt_content.lt_desc::text"
        ).get()
        city = response.css(
            "#maincol > div.row.single-property > div.span3.maininfo > h3::text"
        ).get()
        address = response.css("#det_indirizzo > span::text").get()
        try:
            address = address.strip() + ", " + city
        except:
            pass
        property_type = response.css(
            "#maincol > div.row.single-property > div.span3.maininfo > ul > li:nth-child(1)::text"
        ).get()
        if "Appartamento" in property_type:
            property_type = "apartment"
        else:
            property_type = "house"
        square_meters = response.css(
            "#maincol > div.row.single-property > div.span3.maininfo > ul > li:nth-child(3)::text"
        ).get()
        try:
            square_meters = int(square_meters.split("Mq")[0])
        except:
            pass
        room_count = None
        try:
            room_count = int(response.css("#det_camere > span::text").get())
        except:
            pass
        if room_count is None:
            room_count = int(response.css("#det_vani > span::text").get())
        bathroom_count = int(response.css("#det_bagni > span::text").get())
        images_all= response.css("img::attr(src)").extract()
        images = ["0"]*100
        for i in range(len(images_all)):
            if 'agestanet' in images_all[i]:
                images[i] = images_all[i]
        j=0
        while "0" in images:
            if images[j] == "0":
                images.pop(j)
            else:
                j=j+1
        images.pop(0)
        external_images_count = len(images)
        rent = (
            response.css(
                "#maincol > div.row.single-property > div.span3.maininfo > span.price.colore1::text"
            )
            .get()
            .split(" ")[1]
        )
        if "." in rent:
            rent = rent.replace(".", "")
        rent = int(rent)
        currency = "EUR"
        floor = response.css("#det_piano > span::text").get()
        try:
            if "/" in floor:
                floor = floor.split("/")[0]
        except:
            pass
        parking = response.css("#det_garage > strong::text").get()
        if parking is not None:
            parking = True
        else:
            parking = False
        elevator = response.css("#det_ascensore > strong::text").get()
        if elevator is not None:
            elevator = True
        else:
            elevator = False
        balcony = response.css("#det_balcone > span::text").get()
        if balcony is not None:
            balcony = True
        else:
            balcony = False
        terrace = response.css("#det_terrazza > span::text").get()
        if terrace is not None:
            terrace = True
        else:
            terrace = False
        latitude = response.css("#maincol > div.lt_tab > div > div.lt_content.lt_mappa > div > iframe::attr(src)").get().split("q&q=")[1].split(",")[0]
        longitude = response.css("#maincol > div.lt_tab > div > div.lt_content.lt_mappa > div > iframe::attr(src)").get().split("&sll")[0].split(",")[1]
        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode",zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude",latitude)
        item_loader.add_value("longitude",longitude)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("floor", floor)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("landlord_name", "bielleimmobiliare")
        item_loader.add_value("landlord_email", "+393288859310")
        item_loader.add_value("landlord_phone", "+390957935082")

        yield item_loader.load_item()