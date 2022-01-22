import scrapy
from ..helper import extract_location_from_coordinates, remove_white_spaces, extract_location_from_address
from ..loaders import ListingLoader
import dateutil.parser
import re
#created by: Abdulrahman Moharram
#fixed by: Adham Mansour

class KwpropertySpider(scrapy.Spider):
    name = 'kwproperty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['kwproperty.com']
    start_urls = ['https://www.kwproperty.com/browse.asp']
    position = 1

    def parse(self, response):
        for url in response.css(".listing-footer a.btn-primary::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_page)
        next_page = response.css("a.pager-next::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse)

    def parse_page(self, response):
        title = response.css(".listing-title::text").get()
        property_type = make_prop(title)
        if property_type is None:
            property_type = 'apartment'
        if property_type != "dont_scrape":
            address = response.css("h1.listing-address::text").get()
            room_count = response.xpath('//div[contains(text(),"Bedrooms")]/span/text()').get()
            city = response.css("h1.listing-address::text").getall()[1]
            bathroom_count = response.xpath('//div[contains(text(),"Bathrooms")]/span/text()').get()
            utilities = response.css("div.listing-desc ::text").re("\$.*utilities")
            rent = int(
                response.css(".listing-price::text").get().strip().replace("/mo", "").replace("$", "").replace(",", ""))
            description = remove_white_spaces(" ".join(response.css(".listing-desc p::text").getall()[0:-4]))
            images = response.css(".p-2.text-center a::attr(href)").getall()
            square_meters = response.xpath('//div[contains(text(),"Sq")]/span/text()').get()
            available_date = response.css(".listing-avail::text").get().replace("Available:", "").strip()
            balcony, dishwasher, washing_machine, elevator, parking, swimming_pool, pets_allowed = fetch_amenities(
                response.css("ul.feature-list li::text").getall())

            b = response.css("div.listing-desc ::text").re("[Bb]achelor")

            if available_date:
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")

            if square_meters:
                square_meters = int(square_meters)
            if room_count:
                room_count = int(float(room_count))
            if b:
                property_type = 'studio'
                room_count = 1

            if utilities:
                utilities = int(re.findall("\$[0-9]+", utilities[0])[0].replace("$", ""))

            if bathroom_count:
                bathroom_count = int(float(bathroom_count))

            if not len(images):
                images = [response.urljoin(i) for i in response.css(".listing-photo-wrapper img::attr(src)").getall()]

            if not description:
                description = remove_white_spaces(" ".join(response.css(".listing-desc div::text").getall()))

            description = re.sub("K-*W [Pp]roperty [Mm]anagement [Cc]orp.", "", description)

            if not description:
                description = remove_white_spaces(re.sub("K-*W [Pp]roperty [Mm]anagement [Cc]orp.", "",
                                                         " ".join(response.css(".listing-desc span ::text").getall())))

            item = ListingLoader(response=response)

            longitude, latitude = extract_location_from_address(address + ", " + city)
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)
            item.add_value("zipcode", zipcode)
            item.add_value("latitude", str(latitude))
            item.add_value("longitude", str(longitude))
            item.add_value("external_link", response.url)
            item.add_value("external_source", self.external_source)
            item.add_value("external_id", re.findall("id=[0-9]+", response.url)[0].replace("id=", ""))
            item.add_value("position", self.position)  # Int
            item.add_value("title", title)
            item.add_value("address", address)
            item.add_value("city", city)
            item.add_value("property_type", property_type)
            item.add_value("square_meters", square_meters)
            item.add_value("room_count", room_count)
            item.add_value("bathroom_count", bathroom_count)
            item.add_value("description", description)
            item.add_value("pets_allowed", pets_allowed)
            item.add_value("available_date", available_date)
            item.add_value("currency", "CAD")
            item.add_value("parking", parking)
            item.add_value("images", images)
            item.add_value("balcony", balcony)
            item.add_value("elevator", elevator)
            item.add_value("rent", rent)
            item.add_value("dishwasher", dishwasher)
            item.add_value("washing_machine", washing_machine)
            item.add_value("swimming_pool", swimming_pool)
            item.add_value("utilities", utilities)
            item.add_value("landlord_name", 'K-W PROPERTY MANAGEMENT CORP.')
            item.add_value("landlord_email", 'kwp@kwproperty.com')
            item.add_value("landlord_phone", '519-954-8082')
            self.position += 1
            if 'business' not in title.lower() and 'commercial' not in title.lower().lower() and room_count:
                yield item.load_item()


def make_prop(val):
    apartments = ['semi', 'apartment', 'condo', '2-storey', 'fourplex', 'condo apt', '3-storey', 'condo townhouse',
                  'co-op apt', 'loft', 'bungaloft', '2 1/2 storey']
    houses = ['detached', 'house', 'twnhouse', 'townhouse', 'bungalow', 'multi-level', 'townhome']
    studios = ['studio', 'bachelor', 'student', 'bedrooms']
    commericals = ['storage', 'parking', 'shop', 'office']
    if not val:
        return ''
    val = val.lower()

    for commerical in commericals:
        if commerical in val:
            return 'dont_scrape'

    for house in houses:
        if house in val:
            return 'house'
    for aprt in apartments:
        if aprt in val:
            return 'apartment'
    for studio in studios:
        if studio in val:
            return 'studio'


def fetch_amenities(l):
    balcony, dishwasher, washing_machine, elevator, parking, swimming_pool, pets_allowed = '', '', '', '', '', '', ''
    for i in l:
        if i:
            if not i:
                continue
            i = i.lower()
            if 'balcon' in i:
                balcony = True
            elif 'dishwasher' in i:
                diswasher = True
            elif 'washer' in i or 'laundry' in i:
                washing_machine = True
            elif 'parking' in i or 'garage' in i:
                parking = True
            elif 'elevator' in i:
                elevator = True
            elif 'pool' in i:
                swimming_pool = True
            elif 'pets' in i:
                pets_allowed = True
            elif 'unfurnished' in i:
                furnished = False
            elif 'furnished' in i:
                furnished = True
    return balcony, dishwasher, washing_machine, elevator, parking, swimming_pool, pets_allowed
