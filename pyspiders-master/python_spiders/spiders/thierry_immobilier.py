# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser


class MySpider(Spider):
    name = 'thierry_immobilier'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "ThierryImmobilier_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
        # "HTTPERROR_ALLOWED_CODES": [403,302]
    }
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36",
    }

    def start_requests(self):
        start_urls = [
            {"url": ["https://www.thierry-immobilier.fr/fr/locations"]},
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers
                )
                
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'article')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = f"https://www.thierry-immobilier.fr/fr/map/mini-fiche/Location/{page}/normal/mb.loyerCcTtcMensuel%7Casc"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type = "".join(response.url)
        if property_type and "maison" in property_type.lower():
            item_loader.add_value("property_type", "house")
        elif property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type", "apartment")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value(
                "external_id", external_id.split(":")[-1].strip())

        rent = response.xpath(
            "//span[@class='prix has_sup']/text()[contains(.,'Prix')]").get()
        if rent:
            rent = rent.split(":")[1]
            if rent and "€" in rent:
                rent = rent.split("€")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        adres = "***".join(response.xpath(
            "//script[contains(.,'streetAddress')]/text()").getall())
        if adres:
            adres = adres.split(
                "***")[0].split("streetAddress")[-1].split(",")[0].replace('"', "").replace(":", "")
            if adres:
                item_loader.add_value("address", adres)
        adrescheck = item_loader.get_output_value("address")
        if not adrescheck:
            adres = response.xpath("//h1/text()").get()
            if adres:
                item_loader.add_value("address", adres.split("à")[-1].strip().split("€")[0].strip(
                ).split(" ")[0]+" "+adres.split("à")[-1].strip().split("€")[0].strip().split(" ")[1])
        city = "***".join(response.xpath(
            "//script[contains(.,'addressLocality')]/text()").getall())
        if city:
            city = city.split(
                "***")[0].split("addressLocality")[-1].split(",")[0].replace('"', "").replace(":", "")
            if city:
                item_loader.add_value("city", city)
        citycheck = item_loader.get_output_value("city")
        if not citycheck:
            city = item_loader.get_output_value("address")
            if city:
                city = city.split(" ")[0]
                if not city == "Le":
                    item_loader.add_value("city", city)
                extercity = item_loader.get_output_value("external_id")
                if extercity == "14599":
                    item_loader.add_value("city", "Le Croisic")
        zipcode = "***".join(response.xpath(
            "//script[contains(.,'postalCode')]/text()").getall())
        if zipcode:
            zipcode = zipcode.split("***")[0].split("postalCode")[-1].split(",")[0].replace(
                '"', "").replace(":", "").split("}")[0].replace("\n", "").strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
        zipcodecheck = item_loader.get_output_value("zipcode")
        if not zipcodecheck:
            zipcode = item_loader.get_output_value("address")
            if zipcode:
                zipcode = zipcode.split(" ")[-1]
                if not zipcode == "Croisic":
                    item_loader.add_value("zipcode", zipcode)
                extercity = item_loader.get_output_value("external_id")
                if extercity == "14599":
                    item_loader.add_value("zipcode", "44490")

        desc = response.xpath(
            "//h2[.='À propos']/following-sibling::div//text()").get()
        if desc:
            item_loader.add_value("description", desc)

        deposit = "".join(response.xpath(
            "//div[contains(.,'Dépôt de garantie')]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.split(
                ":")[-1].split("€")[0].strip().replace(" ", ""))

        room_count = response.xpath(
            "//span[.='Nombre de chambres :']/following-sibling::b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath(
            "//span[.='Salle de bains :']/following-sibling::b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath(
            "//span[.='Surface habitable :']/following-sibling::b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(".")[0])

        elevator = response.xpath(
            "//span[.='Ascenseur :']/following-sibling::b/text()").get()
        if elevator and elevator == "Oui":
            item_loader.add_value("elevator", True)

        floor = response.xpath(
            "//span[.='Étage :']/following-sibling::b/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        available_date = response.xpath(
            "//span[.='Disponible le :']/following-sibling::b/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                item_loader.add_value(
                    "available_date", date_parsed.strftime("%Y-%m-%d"))

        energy_label = response.xpath(
            "//div[@class='bilan_conso']/div[1]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("_")[-1])

        images = response.xpath("//a[@class='item photo']/@href").getall()
        if images:
            item_loader.add_value("images", images)

            item_loader.add_value("landlord_name", "Thierry Immobilier")

        yield item_loader.load_item()
