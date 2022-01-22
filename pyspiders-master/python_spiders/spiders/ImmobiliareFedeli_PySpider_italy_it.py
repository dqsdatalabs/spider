import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, property_type_lookup, extract_number_only, extract_location_from_address, extract_location_from_coordinates
from urllib.parse import urlparse
import requests


class ImmobiliareFedeli_PySpider_italy_it(scrapy.Spider):
    name = "immobiliare_fedeli_it"
    allowed_domains = ["rebeccafedeli.com"]
    start_urls = ["https://www.rebeccafedeli.com/index.php?pagina=immobili&categoria=Affitti"]
    execution_type = "testing"
    country = "italy"
    locale = "it"
    thousand_separator = '.'
    scale_separator = ','
    position = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".grid-offer-front"):
            property_type = property_type_lookup.get(listing.css(".estate-type::text").get(), None)
            if property_type is None:
                continue
            rent, currency = extract_rent_currency(listing.css(".grid-price::text").get(), "italy", ImmobiliareFedeli_PySpider_italy_it)
            if rent > 10000: #Means it is for sale
                continue
            url = listing.css(".grid-offer-photo a::attr('href')").get()
            base_url = "{uri.scheme}://{uri.netloc}/".format(uri=urlparse(response.request.url))
            bathroom_count = int(extract_number_only("".join(listing.css(".grid-baths::text").extract())))
            yield scrapy.Request(
                base_url + url,
                callback=self.populate_item,
                meta={ "rent": rent, "currency": currency, "property_type": property_type, "bathroom_count": bathroom_count }
            )

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        currency = response.meta.get("currency", "EUR")
        rent = response.meta.get("rent", None)
        property_type = response.meta.get("property_type", None)
        bathroom_count = response.meta.get("bathroom_count", None)

        external_id = response.css(".subtitle-margin::text").get().split(":")[-1].strip()
        title = response.css(".details-title h3::text").get().strip()
        landlord_phone = response.css(".details-parameters-cont > span::text").get()
        images = response.css(".title-separator-primary + div").css("img::attr('src')").extract()

        params = {}
        for param in response.css(".details-parameters-cont"):
            key = param.css(".details-parameters-name::text").get()
            val = param.css(".details-parameters-val::text").get()
            if key and val:
                params[key] = val
        square_meters = int(extract_number_only(params.get("Superficie", 0)))
        city = params.get("Città", None)

        ticks = {}
        for param in response.css(".details-ticks li::text").extract():
            if "PIANO" in param:
                ticks["floor"] = str(extract_number_only(param.split(":")[-1]))
            elif "LOCALI" in param:
                ticks["room_count"] = int(extract_number_only(param.split(":")[-1]))
            elif "ARREDATO" in param:
                ticks["furnished"] = True
            elif "ASCENSORE" in param:
                ticks["elevator"] = "SI" in param
        floor = ticks.get("floor", None)
        room_count = ticks.get("room_count", 0)
        furnished = ticks.get("furnished", False)
        elevator = ticks.get("elevator", None)

        description_lines = [line.strip() for line in response.css(".details-desc::text").extract() if line.strip()]
        address = description_lines.pop(0)
        description = "\r\n".join(description_lines)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", f"ImmobiliareFedeli_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", " ".join(map(str.capitalize, self.name.split("_")[:-1])))
        item_loader.add_value("landlord_email", response.css(".top-mail .top-bar-text::text").get())
        item_loader.add_value("description", description)

        address = ", ".join([address, city])
        longitude, latitude = extract_location_from_address(address)
        zipcode, _, _ = extract_location_from_coordinates(longitude, latitude)

        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("longitude", str(longitude))
        item_loader.add_value("latitude", str(latitude))
        item_loader.add_value("zipcode", zipcode)

        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        if external_id:
            item_loader.add_value("external_id", external_id)
        if title:
            item_loader.add_value("title", title)
        if floor:
            item_loader.add_value("floor", floor)            
        if furnished:
            item_loader.add_value("furnished", furnished)
        if elevator is not None:
            item_loader.add_value("elevator", elevator)

        self.position += 1

        yield item_loader.load_item()
